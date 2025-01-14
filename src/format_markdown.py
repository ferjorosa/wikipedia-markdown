import re
from pathlib import Path
from typing import Union

import pandas as pd
import swifter  # noqa: F401
from tqdm import tqdm


def to_markdown(input: Union[str, pd.DataFrame], parallel: bool = False):
    if isinstance(input, str):
        return _format(input)

    elif isinstance(input, pd.DataFrame):
        if "raw_text" not in input.columns:
            raise ValueError("DataFrame must contain a 'raw_text' column")

        if parallel:
            # Use swifter for parallel processing with tqdm progress bar
            tqdm.pandas(desc="Processing")  # Enable tqdm for pandas
            input["markdown_text"] = (
                input["raw_text"].swifter.progress_bar(True).apply(_format)
            )
        else:
            # Sequential processing with tqdm progress bar
            tqdm.pandas(desc="Processing")  # Enable tqdm for pandas
            input["markdown_text"] = input["raw_text"].progress_apply(_format)

        return input

    else:
        raise TypeError("Input must be a string or a pandas DataFrame")


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
    input_path = Path("../data/parsed/articles.parquet")
    output_path = Path("../data/parsed/articles_markdown.parquet")
    target_article_id = 1

    # Load the DataFrame
    df = pd.read_parquet(input_path)

    # Example 1: Fetch a specific article by ID and clean its text
    article = df[df["id"] == target_article_id].iloc[0]
    markdown_text = to_markdown(article["raw_text"])
    print(markdown_text)

    # Example 2: Process all articles
    print("Processing all articles...")
    df = to_markdown(df, parallel=True)  # Use parallel processing
    df.to_parquet(output_path)
    print(f"Processed data saved to {output_path}")
