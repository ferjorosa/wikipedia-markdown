from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from tqdm import tqdm
from transformers import PreTrainedTokenizerFast

from src.convert_to_markdown import (
    convert_long_text_to_markdown,
    convert_text_to_markdown,
)
from src.utils.database import insert_row
from src.utils.tokenizer import count_tokens


def parallel_process_dataframe(
    data: pd.DataFrame,
    model_openrouter: str,
    template: str,
    tokenizer: PreTrainedTokenizerFast,
    model_hf: str,
    db_path: str,
    max_tokens: int = 7000,
    max_workers: int = 4,
) -> None:
    """
    Process a DataFrame in parallel to convert text to markdown and insert it
    into a database. Uses different conversion methods for short and long text.

    Args:
        data (pd.DataFrame): DataFrame with rows to process.
        model_openrouter (str): Model for markdown conversion.
        template (str): Template for markdown conversion.
        tokenizer (PreTrainedTokenizerFast): Tokenizer for counting tokens.
        model_hf (str): Hugging Face model identifier.
        db_path (str): SQLite database path.
        max_tokens (int): Max tokens allowed for long text (default: 7000).
        max_workers (int): Max threads for parallel processing (default: 4).

    Returns:
        None
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                _process_row,
                row,
                model_openrouter,
                template,
                tokenizer,
                model_hf,
                db_path,
                max_tokens,
            )
            for _, row in data.iterrows()
        ]

        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Processing rows"
        ):
            try:
                future.result()
            except Exception as e:
                print(f"An error occurred: {e}")


def _process_row(
    row: pd.Series,
    model_openrouter: str,
    template: str,
    tokenizer: PreTrainedTokenizerFast,
    model_hf: str,
    db_path: str,
    max_tokens: int = 7000,
) -> None:
    """
    Convert a DataFrame row's text to markdown and insert it into a database.

    Args:
        row (pd.Series): A DataFrame row with 'id', 'title', and 'text' fields.
        model_openrouter (str): Model for markdown conversion.
        template (str): Template for markdown conversion.
        tokenizer (PreTrainedTokenizerFast): Tokenizer for counting tokens.
        model_hf (str): Hugging Face model identifier.
        db_path (str): SQLite database path.
        max_tokens (int): Max tokens for long text (default: 7000).

    Returns:
        None
    """
    n_tokens = count_tokens(tokenizer, row["text"])

    if n_tokens <= max_tokens:
        markdown_text = convert_text_to_markdown(
            model_openrouter=model_openrouter,
            raw_text=row["text"],
            template=template,
        )
    else:
        markdown_text = convert_long_text_to_markdown(
            model_openrouter=model_openrouter,
            raw_text=row["text"],
            template=template,
            tokenizer=tokenizer,
            max_tokens=max_tokens,
        )

    insert_row(
        db_path=db_path,
        id=int(row["id"]),
        title=row["title"],
        raw_text=row["text"],
        markdown_text=markdown_text,
        raw_text_tokens=n_tokens,
        markdown_text_tokens=count_tokens(tokenizer, markdown_text),
        model=model_hf,
    )
