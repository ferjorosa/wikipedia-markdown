import bz2
import re
from pathlib import Path
from time import sleep
from typing import Dict, Iterator, Optional, Union

import pandas as pd
import wikitextparser as wtp
from bs4 import BeautifulSoup
from tqdm import tqdm

from src.utils.wiki_table import wiki_table_to_html


def fetch_article_by_id(
    filename: Union[str, Path], target_id: int, domain: str, clean_text: bool = False
) -> Optional[Dict[str, str]]:
    """Retrieve the text, title, and ID of a specific Wikipedia article by its ID
    from a bz2-compressed XML dump file.

    Args:
        filename: Path to the bz2 compressed XML dump file.
        target_id: The ID of the article to retrieve.
        clean_text: If True, clean the article text before returning.

    Returns:
        A dictionary containing the article's title, content, and ID if found,
        otherwise None.
    """
    for article in _iterate_articles(
        filename, domain, clean_text=False
    ):  # Don't clean text here
        if article and int(article["id"]) == target_id:
            if clean_text:
                article["raw_text"] = _clean_article_text(article["raw_text"])
            return article
    print(f"Article with ID {target_id} not found in the dump file.")
    return None


def process_articles_to_parquet(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
    domain: str,
    clean_text: bool = False,
) -> None:
    """Process all articles in a Wikipedia XML dump file and save them into a
    Parquet file.

    Args:
        input_path: Path to the bzip2 compressed XML dump file.
        output_path: Path to the output Parquet file.
        clean_text: If True, clean the article text before saving.
    """
    input_path, output_path = Path(input_path), Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total_pages = _count_pages_in_file(input_path)
    print(f"Total unique articles to process: {total_pages}")
    sleep(0.1)  # Allow print to be shown before tqdm

    articles = []
    for article in tqdm(
        _iterate_articles(input_path, domain, clean_text=clean_text),
        total=total_pages,
        desc="Processing articles",
        unit="article",
    ):
        if article:
            articles.append(article)

    df = pd.DataFrame(articles)
    df["id"] = df["id"].astype(int)
    df.to_parquet(output_path, index=False)
    print(f"Processed data saved to {output_path}")


def _clean_article_text(article_text: str) -> str:
    """Clean the text of a Wikipedia article by removing wikitext markup and HTML tags.

    Args:
        article_text: The raw text of the article.

    Returns:
        The cleaned text of the article.
    """
    # Parse the wikitext and convert it to plain text
    article_text_parsed = wtp.parse(article_text)
    article_text_plain = article_text_parsed.plain_text(
        replace_tables=wiki_table_to_html
    )

    # Parse the HTML text using BeautifulSoup
    soup = BeautifulSoup(article_text_plain, features="lxml")

    # Remove all <ref> tags and their contents
    for ref in soup.find_all("ref"):
        ref.decompose()

    # Remove all <sup> tags and their contents
    for ref in soup.find_all("sup"):
        ref.decompose()

    # Remove all <sub> tags and their contents
    for ref in soup.find_all("sub"):
        ref.decompose()

    # Extract and store all tables (to avoid being removed by BS4)
    tables = soup.find_all("table")
    table_placeholders = []
    for i, table in enumerate(tables):
        placeholder = f"\n##TABLE_PLACEHOLDER_{i}##\n"
        table_placeholders.append((placeholder, str(table)))
        table.replace_with(placeholder)

    # Extract and store all <syntaxhighlight> tags (to avoid being removed by BS4)
    syntax_highlight_placeholders = []
    for i, syntaxhighlight in enumerate(soup.find_all("syntaxhighlight")):
        placeholder = f"\n##SYNTAXHIGHLIGHT_PLACEHOLDER_{i}##\n"
        syntax_highlight_placeholders.append((placeholder, str(syntaxhighlight)))
        syntaxhighlight.replace_with(placeholder)

    # For all other tags, replace them with their text content
    for tag in soup.find_all(True):
        tag.replace_with(tag.get_text())

    # Get the cleaned text
    cleaned_text = str(soup)

    # Separate titles using Regex (this could affect HTML tables)
    pattern = r"(^|\n)(={1,6})\s*([^=\n]+?)\s*\2(\n|$)"
    replacement = (
        lambda match: f"{match.group(1)}\n\n{match.group(2)}{match.group(3)}"
        f"{match.group(2)}\n\n{match.group(4)}"
    )
    cleaned_text = re.sub(pattern, replacement, cleaned_text)

    # 2 newlines max
    cleaned_text = re.sub(r"\n{3,}", "\n\n", cleaned_text)

    # Reinsert the tables with newlines around them
    for placeholder, table_html in table_placeholders:
        cleaned_text = cleaned_text.replace(placeholder, f"\n{table_html}\n")

    # Reinsert the <syntaxhighlight> tags with newlines around them
    for placeholder, syntaxhighlight_html in syntax_highlight_placeholders:
        cleaned_text = cleaned_text.replace(placeholder, f"\n{syntaxhighlight_html}\n")

    return cleaned_text


def _generate_url(title: str, domain: str) -> str:
    """Generate the Wikipedia URL for a given article title.

    Args:
        title: The title of the Wikipedia article.
        domain: The Wikipedia domain to use (e.g., "en", "simple").

    Returns:
        The full URL to the Wikipedia article.
    """
    encoded_title = title.replace(" ", "_")
    return f"https://{domain}.wikipedia.org/wiki/{encoded_title}"


def _iterate_articles(
    filename: Union[str, Path], domain: str, clean_text: bool = False
) -> Iterator[Optional[Dict[str, str]]]:
    """Iterate over articles in a bz2-compressed XML dump file.

    Args:
        filename: Path to the bz2 compressed XML dump file.
        clean_text: If True, clean the article text before yielding.

    Yields:
        A dictionary containing the article's title, content, and ID if valid,
        otherwise None.
    """
    with bz2.open(filename, "rt", encoding="utf-8") as infile:
        article = ""
        for line in infile:
            if "<page>" in line:
                article = ""
            elif "</page>" in line:
                parsed_article = _parse_article(article, domain)
                if parsed_article and clean_text:
                    parsed_article["raw_text"] = _clean_article_text(
                        parsed_article["raw_text"]
                    )
                yield parsed_article
            else:
                article += line


def _parse_article(text: str, domain: str) -> Optional[Dict[str, str]]:
    """Parse a Wikipedia article chunk and extract relevant information.

    Args:
        text: Raw XML chunk containing a Wikipedia article.

    Returns:
        Dictionary containing article title, content, and ID if valid.
        Returns None if the article should be skipped.
    """
    try:
        if '<redirect title="' in text or "(disambiguation)" in text:
            return None

        title = text.split("<title>")[1].split("</title>")[0]
        if ":" in title:
            return None

        article_id = text.split("<id>")[1].split("</id>")[0]
        content = text.split("</text")[0].split("<text")[1].split(">", maxsplit=1)[1]
        content = f"= {title.strip()} =\n{content.strip()}"

        # Generate the URL for the article
        url = _generate_url(title, domain)

        return {
            "id": article_id.strip(),
            "title": title.strip(),
            "url": url,
            "raw_text": content,
        }
    except Exception as e:
        print(f"Error parsing article: {e}")
        return None


def _count_pages_in_file(filename: Union[str, Path]) -> int:
    """Count the total number of pages in a bzip2 compressed XML file.

    Args:
        filename: Path to the bzip2 compressed XML file.

    Returns:
        Total number of pages found in the file.
    """
    with bz2.open(filename, "rt", encoding="utf-8") as infile:
        # Use tqdm with a description, unit, and optional total (if known)
        return sum(
            1
            for line in tqdm(
                infile,
                desc="Counting pages",
                unit="page",
            )
            if "<page>" in line
        )


if __name__ == "__main__":
    # Define paths and target article ID
    raw_path = Path("../data/raw")
    input_file = "articles.xml.bz2"
    input_path = raw_path / input_file
    output_path = Path("../data/parsed/articles.parquet")
    domain = "simple"

    # Example 1: Fetch a specific article by ID and clean its text
    # target_article_id = 3077 # Article with multiple tables
    # target_article_id = 44678 # Article with code blocks
    target_article_id = 431  # canada table error
    article = fetch_article_by_id(
        filename=input_path, target_id=target_article_id, domain=domain, clean_text=True
    )
    if article:
        print(f"ID: {article['id']}")
        print(f"Title: {article['title']}")
        print(f"URL: {article['url']}")
        print(f"Parsed Text:\n{article['raw_text']}")
    else:
        print(f"Article with ID {target_article_id} not found.")

    # Example 2: Process all articles, clean their text, and save to Parquet
    process_articles_to_parquet(
        input_path=input_path, output_path=output_path, domain=domain, clean_text=True
    )
