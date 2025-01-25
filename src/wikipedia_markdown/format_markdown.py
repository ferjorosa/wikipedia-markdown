import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from os import getenv
from pathlib import Path
from typing import Any, Dict, Optional, Union

from tqdm import tqdm
from transformers import AutoTokenizer, PreTrainedTokenizerFast

from wikipedia_markdown.utils.database import (
    get_all_ids,
    get_rows_from_ids,
    update_markdown_text_batch,
)
from wikipedia_markdown.utils.tokenizer import count_tokens
from wikipedia_markdown.utils.yaml import load_yaml


def format_article(article: str) -> str:
    return _format(article)


def format_articles(
    db_path: Union[str, Path],
    tokenizer: PreTrainedTokenizerFast,
    max_workers: int = 4,
    batch_size: int = 1000,
    debug: bool = False,
):
    # Step 1: Get all article IDs from the database
    all_ids = get_all_ids(db_path)
    print(f"Total articles to process: {len(all_ids)}")

    # Step 2: Split IDs into batches
    batches = [all_ids[i : i + batch_size] for i in range(0, len(all_ids), batch_size)]

    # Step 3: Process each batch
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for batch in tqdm(batches, desc="Formatting Article Batches"):
            # Step 3.1: Retrieve rows for the current chunk
            rows = get_rows_from_ids(
                db_path=db_path, ids=batch, columns=["id", "raw_text"]
            )

            # Step 3.2: Format articles to Markdown
            formatted_articles = []
            futures = []
            for row in rows:
                future = executor.submit(_format_article_text, row, tokenizer)
                futures.append(future)

            # Step 3.3: Collect results
            for future in as_completed(futures):
                result = future.result()
                if result:
                    formatted_articles.append(result)

            # Step 3.4: Update the database with formatted articles
            if formatted_articles:
                update_markdown_text_batch(db_path, formatted_articles, debug=debug)


def _format_article_text(
    row: Dict[str, Any], tokenizer: PreTrainedTokenizerFast
) -> Optional[Dict[str, Union[int, str]]]:
    """
    Format a single article to Markdown and count its tokens.

    Args:
        row (Dict[str, Any]): Dictionary containing the article's id and raw_text.
        tokenizer (PreTrainedTokenizerFast): Tokenizer for counting tokens.

    Returns:
        Optional[Dict[str, Union[int, str]]]: Dictionary containing the article's id,
                                              markdown_text, and markdown_text_tokens.
    """
    try:
        markdown_text = _format(row["raw_text"])
        markdown_text_tokens = count_tokens(tokenizer, markdown_text)
        return {
            "id": row["id"],
            "markdown_text": markdown_text,
            "markdown_text_tokens": markdown_text_tokens,
        }
    except Exception as e:
        print(f"Error formatting article {row['id']}: {e}")
        return None


def _format(cleaned_text: str) -> str:
    """Convert cleaned Wikipedia text to Markdown format.

    Args:
        cleaned_text: The cleaned text of the article.

    Returns:
        The text in Markdown format.
    """
    # Convert titles and code blocks
    cleaned_text = _format_title(cleaned_text)
    cleaned_text = _format_code_blocks(cleaned_text)
    cleaned_text = _fix_nested_lists(cleaned_text)

    # Remove all newlines before the FIRST title that starts with #
    cleaned_text = re.sub(r"^\n*(?=#)", "", cleaned_text)

    return cleaned_text


def _format_title(text: str) -> str:
    """Convert Wikipedia-style titles to Markdown headings.

    Args:
        text: The input text containing Wikipedia-style titles.

    Returns:
        The text with titles converted to Markdown headings.
    """

    def replace_title(match):
        level = len(match.group(2))  # Number of '=' signs
        title = match.group(3).strip()
        return f"{'#' * level} {title}\n"

    # Regex pattern to match Wikipedia-style titles
    title_pattern = r"(^|\n)(={1,6})\s*([^=\n]+?)\s*\2(\n|$)"
    return re.sub(title_pattern, replace_title, text)


def _format_code_blocks(text: str) -> str:
    """Convert <syntaxhighlight> tags to Markdown code blocks.

    Args:
        text: The input text containing <syntaxhighlight> tags.

    Returns:
        The text with <syntaxhighlight> tags converted to Markdown code blocks.
    """

    def replace_code_block(match):
        lang = match.group(1)
        code = match.group(2).strip()
        return f"```{lang}\n{code}\n```"

    # Regex pattern to match <syntaxhighlight> tags
    code_pattern = r"<syntaxhighlight lang=\"(\w+)\">(.*?)</syntaxhighlight>"
    return re.sub(code_pattern, replace_code_block, text, flags=re.DOTALL)


def _fix_nested_lists(text: str) -> str:

    # Step 2: Fix nested list formatting for any number of levels
    return re.sub(
        r"^(\*+)(.*)",
        lambda match: "  " * (len(match.group(1)) - 1) + "*" + match.group(2),
        text,
        flags=re.MULTILINE,
    )


if __name__ == "__main__":
    # Define paths and tokenizer
    base_path = Path("../../")
    config = load_yaml(base_path / "config.yaml")
    db_path = base_path / config["data_folder"] / config["db_file"]
    huggingface_token = getenv("HUGGINGFACE_TOKEN")
    tokenizer = AutoTokenizer.from_pretrained(
        config["model_hf"], token=huggingface_token
    )

    # Format all articles
    cpu_count = os.cpu_count() or 4
    format_articles(
        db_path=db_path,
        tokenizer=tokenizer,
        max_workers=8,
        batch_size=1000,
        debug=False,
    )
