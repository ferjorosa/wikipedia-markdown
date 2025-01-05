import bz2
from pathlib import Path
from time import sleep
from typing import Dict, Iterator, Optional, Union

import pandas as pd
import wikitextparser as wtp
from bs4 import BeautifulSoup
from tqdm import tqdm


def fetch_article_by_id(
    filename: Union[str, Path], target_id: int, clean_text: bool = False
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
    for article in _iterate_articles(filename, clean_text=clean_text):
        if article and int(article["id"]) == target_id:
            return article
    print(f"Article with ID {target_id} not found in the dump file.")
    return None


def process_articles_to_parquet(
    input_path: Union[str, Path],
    output_path: Union[str, Path],
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
        _iterate_articles(input_path, clean_text=clean_text),
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
    article_text_plain = article_text_parsed.plain_text()

    # Parse the HTML text using BeautifulSoup
    soup = BeautifulSoup(article_text_plain, features="lxml")

    # Remove all <ref> tags and their contents
    for ref in soup.find_all("ref"):
        ref.decompose()  # This removes the tag and its contents

    # For all other tags, replace them with their text content
    for tag in soup.find_all(True):  # True means "find all tags"
        if tag.name != "ref":  # Skip <ref> tags (already removed)
            tag.replace_with(tag.get_text())

    # Get the cleaned text
    cleaned_text = str(soup)

    return cleaned_text


def _iterate_articles(
    filename: Union[str, Path], clean_text: bool = False
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
                parsed_article = _parse_article(article)
                if parsed_article and clean_text:
                    parsed_article["text"] = _clean_article_text(parsed_article["text"])
                yield parsed_article
            else:
                article += line


def _parse_article(text: str) -> Optional[Dict[str, str]]:
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

        return {"title": title.strip(), "text": content, "id": article_id.strip()}
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
    input_file = "simplewiki-latest-pages-articles-multistream.xml.bz2"
    input_path = raw_path / input_file
    output_path = Path("../data/processed/articles.parquet")

    # Example 1: Fetch a specific article by ID and clean its text
    target_article_id = 3077  # Replace with the desired article ID
    article = fetch_article_by_id(input_path, target_article_id, clean_text=True)
    if article:
        print(f"Title: {article['title']}")
        print(f"ID: {article['id']}")
        print(f"Cleaned Text:\n{article['text']}")
    else:
        print(f"Article with ID {target_article_id} not found.")

    # Example 2: Process all articles, clean their text, and save to Parquet
    process_articles_to_parquet(input_path, output_path, clean_text=True)