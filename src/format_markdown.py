import re
from pathlib import Path

import pandas as pd


def convert_to_markdown(cleaned_text: str) -> str:
    """Convert cleaned Wikipedia text to Markdown format.

    Args:
        cleaned_text: The cleaned text of the article.

    Returns:
        The text in Markdown format.
    """
    # Convert titles and code blocks
    cleaned_text = _convert_title(cleaned_text)
    cleaned_text = _convert_code_blocks(cleaned_text)

    return cleaned_text


def _convert_title(text: str) -> str:
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


def _convert_code_blocks(text: str) -> str:
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


if __name__ == "__main__":
    input_path = Path("../data/parsed/articles.parquet")
    target_article_id = 44678

    df = pd.read_parquet(input_path)

    article = df[df["id"] == target_article_id].iloc[0]

    markdown_text = convert_to_markdown(article["text"])

    print(markdown_text)
