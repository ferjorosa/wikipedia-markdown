# Note: We use the _iterate_articles in the parse_article method and not in the
# parse_all_articles because we needed to modify the inner logic

import bz2
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from os import getenv
from pathlib import Path
from time import sleep
from typing import Any, Dict, Iterator, Optional, Union

import wikitextparser as wtp
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tqdm import tqdm
from transformers import AutoTokenizer, PreTrainedTokenizerFast

from wikipedia_markdown.utils.database import (
    initialize_db,
    insert_raw_text_batch,
    insert_raw_text_row,
)
from wikipedia_markdown.utils.tokenizer import count_tokens
from wikipedia_markdown.utils.wiki_table import wiki_table_to_html
from wikipedia_markdown.utils.yaml import load_yaml


def parse_article(
    file_path: Union[str, Path], target_id: int, domain: str, clean_text: bool = False
) -> Optional[Dict[str, str]]:
    """Retrieve the text, title, and ID of a specific Wikipedia article by its ID
    from a bz2-compressed XML dump file.

    Args:
        file_path: Path to the bz2 compressed XML dump file.
        target_id: The ID of the article to retrieve.
        clean_text: If True, clean the article text before returning.

    Returns:
        A dictionary containing the article's title, content, and ID if found,
        otherwise None.
    """
    for article in _iterate_articles(
        file_path, domain, clean_text=False
    ):  # Don't clean text here
        if article and int(article["id"]) == target_id:
            if clean_text:
                article["raw_text"] = _clean_article_text(article["raw_text"])
            return article
    print(f"Article with ID {target_id} not found in the dump file.")
    return None


def parse_articles(
    file_path: Union[str, Path],
    db_path: Union[str, Path],
    domain: str,
    tokenizer: PreTrainedTokenizerFast,
    clean_text: bool = True,
    max_workers: int = 4,
) -> None:
    """Process all articles in a Wikipedia XML dump file and save them into a
    SQLite database using a thread pool."""
    initialize_db(db_path)

    total_pages = _count_pages_in_file(file_path)
    print(f"Total unique articles to process: {total_pages}")
    sleep(0.1)  # Allow print to be shown before processing

    def process_and_insert_article(article_text: str) -> None:
        """Helper function to process a single article and insert it into the
        database."""
        parsed_article = _parse_article_text(article_text, domain)
        if parsed_article:
            if clean_text:
                parsed_article["raw_text"] = _clean_article_text(
                    parsed_article["raw_text"]
                )
            insert_raw_text_row(
                db_path=db_path,
                id=parsed_article["id"],
                title=parsed_article["title"],
                url=parsed_article["url"],
                raw_text=parsed_article["raw_text"],
                raw_text_tokens=count_tokens(tokenizer, parsed_article["raw_text"]),
            )

    with (
        bz2.open(file_path, "rt", encoding="utf-8") as infile,
        ThreadPoolExecutor(max_workers=max_workers) as executor,
    ):
        article: list[str] = []
        in_page = False
        futures = []  # Store futures for processed articles

        with tqdm(total=total_pages, desc="Processing Articles") as pbar:
            for line in infile:
                if "<page>" in line:
                    article = []  # Reset the article buffer
                    in_page = True
                elif "</page>" in line and in_page:
                    # Submit the article for processing and insertion
                    article_text = "".join(article)
                    future = executor.submit(process_and_insert_article, article_text)
                    future.add_done_callback(
                        lambda _: pbar.update(1)
                    )  # Update progress bar when future is done
                    futures.append(future)
                    in_page = False
                elif in_page:
                    article.append(line)  # Accumulate lines within <page> and </page>

            # Wait for all futures to complete
            for future in as_completed(futures):
                future.result()  # This will raise any exceptions that occurred


# TODO: Does not work
# TODO: Either way, the idea is to make it a process with batches
# First we generate the batch and then we pass it to a process...
def parse_all_articles_batch(
    file_path: Union[str, Path],
    db_path: Union[str, Path],
    domain: str,
    tokenizer: PreTrainedTokenizerFast,
    clean_text: bool = True,
    max_workers: int = 4,
    batch_size: int = 1000,
    debug: bool = False,
) -> None:
    """Process all articles in a Wikipedia XML dump file and save them into a
    SQLite database using a thread pool."""
    initialize_db(db_path)

    total_pages = _count_pages_in_file(file_path)
    print(f"Total unique articles to process: {total_pages}")
    sleep(0.1)  # Allow print to be shown before processing

    def process_article(article_text: str) -> Optional[Dict[str, Union[int, str]]]:
        """Helper function to process a single article and return its data."""
        parsed_article = _parse_article_text(article_text, domain)
        if parsed_article:
            if clean_text:
                parsed_article["raw_text"] = _clean_article_text(
                    parsed_article["raw_text"]
                )
            parsed_article["raw_text_tokens"] = count_tokens(
                tokenizer, parsed_article["raw_text"]
            )
            return parsed_article
        return None

    with (
        bz2.open(file_path, "rt", encoding="utf-8") as infile,
        ThreadPoolExecutor(max_workers=max_workers) as executor,
    ):
        article: list[str] = []
        in_page = False
        futures = []  # Store futures for processed articles
        batch = []  # Accumulate articles for batch insertion

        with tqdm(total=total_pages, desc="Processing Articles") as pbar:
            for line in infile:
                if "<page>" in line:
                    article = []  # Reset the article buffer
                    in_page = True
                elif "</page>" in line and in_page:
                    # Submit the article for processing
                    article_text = "".join(article)
                    future = executor.submit(process_article, article_text)
                    future.add_done_callback(
                        lambda _: pbar.update(1)
                    )  # Update progress bar when future is done
                    futures.append(future)
                    in_page = False
                elif in_page:
                    article.append(line)  # Accumulate lines within <page> and </page>

            # Process futures and accumulate articles into batches
            for future in as_completed(futures):
                result = future.result()
                if result:
                    batch.append(result)
                    if len(batch) >= batch_size:
                        insert_raw_text_batch(db_path, batch, debug=debug)
                        batch = []  # Reset the batch

            # Insert any remaining articles in the last batch
            if batch:
                insert_raw_text_batch(db_path, batch, debug=debug)


def _parse_article_text(text: str, domain: str) -> Optional[Dict[str, Any]]:
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
            "id": int(article_id.strip()),
            "title": title.strip(),
            "url": url,
            "raw_text": content,
        }
    except Exception as e:
        print(f"Error parsing article: {e}")
        return None


def _clean_article_text(article_text: str) -> str:
    """Clean the text of a Wikipedia article by removing wikitext markup and
    HTML tags.

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
                parsed_article = _parse_article_text(article, domain)
                if parsed_article and clean_text:
                    parsed_article["raw_text"] = _clean_article_text(
                        parsed_article["raw_text"]
                    )
                yield parsed_article
            else:
                article += line


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
    base_path = Path("../../")
    config = load_yaml(base_path / "config.yaml")
    file_path = base_path / config["data_folder"] / config["raw_file"]
    db_path = base_path / config["data_folder"] / config["db_file"]
    domain = "simple"

    # Example 1: Fetch a specific article by ID and clean its text
    # target_article_id = 3077 # Article with multiple tables
    # target_article_id = 44678 # Article with code blocks
    # target_article_id = 431  # canada table error
    # article = parse_article(
    #     file_path=file_path,
    #     target_id=target_article_id,
    #     domain=domain,
    #     clean_text=True
    # )
    # if article:
    #     print(f"ID: {article['id']}")
    #     print(f"Title: {article['title']}")
    #     print(f"URL: {article['url']}")
    #     print(f"Parsed Text:\n{article['raw_text']}")
    # else:
    #     print(f"Article with ID {target_article_id} not found.")

    # Example 2: Process all articles, clean their text, and save to DB
    load_dotenv()
    huggingface_token = getenv("HUGGINGFACE_TOKEN")
    tokenizer = AutoTokenizer.from_pretrained(
        config["model_hf"], token=huggingface_token
    )
    # Disable tokenizer parallelism (going to be called withing ThreadPool)
    os.environ["TOKENIZERS_PARALLELISM"] = "false"

    # Record the start time
    start_time = time.perf_counter()

    # Call the function
    cpu_count = os.cpu_count() or 4
    parse_articles(
        file_path=file_path,
        db_path=db_path,
        domain=domain,
        tokenizer=tokenizer,
        clean_text=True,
        max_workers=cpu_count - 1,
        # batch_size=1000,
    )

    # Record the end time
    end_time = time.perf_counter()

    # Calculate and print the total time taken
    time_taken = end_time - start_time
    print(f"Time taken to execute: {time_taken:.2f} seconds")
