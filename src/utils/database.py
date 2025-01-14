import sqlite3
from pathlib import Path
from typing import Dict, List, Union

import pandas as pd


def initialize_db(db_path: Union[str, Path]):
    """
    Initialize the SQLite database and create the table if it doesn't exist.

    Args:
        db_path (Union[str, Path]): Path to the SQLite database file as a string
                                    or Path object.
    """
    # Convert db_path to a Path object if it's a string
    db_path = Path(db_path) if isinstance(db_path, str) else db_path

    # Create the directory if it doesn't exist
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if db_path.exists():
        print(f"Database already exists at {db_path}")
    else:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY,
                title TEXT,
                url TEXT,
                raw_text TEXT,
                raw_text_tokens INTEGER,
                markdown_text TEXT,
                markdown_text_tokens INTEGER,
                model TEXT,
                llm_cleaned_text TEXT,
                llm_cleaned_text_tokens INTEGER
            )
            """
        )
        conn.commit()
        conn.close()
        print(f"Database initialized successfully at {db_path}")


def filter_rows_in_db(
    df: pd.DataFrame, db_path: Union[str, Path], id_column: str = "id"
) -> pd.DataFrame:
    """
    Filter DataFrame rows to exclude IDs already present in the database.

    Args:
        df (pd.DataFrame): DataFrame to filter.
        db_path (str): Path to the SQLite database file.
        id_column (str): Column containing IDs (default: 'id').

    Returns:
        pd.DataFrame: Filtered DataFrame with IDs not in the database.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    ids = df[id_column].tolist()
    placeholders = ",".join(["?"] * len(ids))
    query = f"SELECT id FROM articles WHERE id IN ({placeholders})"
    cursor.execute(query, ids)
    existing_ids = [row[0] for row in cursor.fetchall()]
    conn.close()
    return df[~df[id_column].isin(existing_ids)]


def insert_row(
    db_path: Union[str, Path],
    id: int,
    title: str,
    url: str,
    raw_text: str,
    raw_text_tokens: int,
    markdown_text: str,
    markdown_text_tokens: int,
    model: str,
    llm_cleaned_text: str,
    llm_cleaned_text_tokens: int,
    debug: bool = False,
):
    """
    Insert a new row into the SQLite database if the ID does not already exist.

    Args:
        db_path (str): Path to the SQLite database file.
        id (int): Unique identifier for the row.
        title (str): Title of the text.
        url (str): URL of the article.
        raw_text (str): Original text.
        raw_text_tokens (int): Token count of raw text.
        markdown_text (str): Transformed Markdown text.
        markdown_text_tokens (int): Token count of Markdown text.
        model (str): Name of the model.
        llm_cleaned_text (str): Cleaned text after LLM processing.
        llm_cleaned_text_tokens (int): Token count of cleaned text.
        debug (bool): If True, print debug messages (default: False).
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR IGNORE INTO articles (
            id, title, url, raw_text, raw_text_tokens,
            markdown_text, markdown_text_tokens, model,
            llm_cleaned_text, llm_cleaned_text_tokens
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            id,
            title,
            url,
            raw_text,
            raw_text_tokens,
            markdown_text,
            markdown_text_tokens,
            model,
            llm_cleaned_text,
            llm_cleaned_text_tokens,
        ),
    )
    if cursor.rowcount > 0:
        if debug:
            print(f"Row inserted successfully with id {id}.")
    else:
        if debug:
            print(f"Row with id {id} already exists. No changes were made.")
    conn.commit()
    conn.close()


def insert_raw_text_row(
    db_path: Union[str, Path],
    id: int,
    title: str,
    url: str,
    raw_text: str,
    raw_text_tokens: int,
    debug: bool = False,
):
    """
    Insert a new row into the SQLite database with only the raw text fields.

    Args:
        db_path (str): Path to the SQLite database file.
        id (int): Unique identifier for the row.
        title (str): Title of the text.
        url (str): URL of the article.
        raw_text (str): Original text.
        raw_text_tokens (int): Token count of raw text.
        debug (bool): If True, print debug messages (default: False).
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR IGNORE INTO articles (
            id, title, url, raw_text, raw_text_tokens
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            id,
            title,
            url,
            raw_text,
            raw_text_tokens,
        ),
    )
    if cursor.rowcount > 0:
        if debug:
            print(f"Row inserted successfully with id {id}.")
    else:
        if debug:
            print(f"Row with id {id} already exists. No changes were made.")
    conn.commit()
    conn.close()


def insert_raw_text_batch(
    db_path: Union[str, Path],
    articles: List[Dict[str, Union[int, str]]],
    debug: bool = False,
) -> None:
    """
    Insert a batch of articles into the SQLite database.

    Args:
        db_path (str): Path to the SQLite database file.
        articles (List[Dict]): List of articles to insert, where each article is a
                               dictionary with keys: id, title, url, raw_text,
                               raw_text_tokens.
        debug (bool): If True, print debug messages (default: False).
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.executemany(
            """
            INSERT OR IGNORE INTO articles (
                id, title, url, raw_text, raw_text_tokens
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    article["id"],
                    article["title"],
                    article["url"],
                    article["raw_text"],
                    article["raw_text_tokens"],
                )
                for article in articles
            ],
        )
        conn.commit()
        if debug:
            print(f"Inserted {len(articles)} articles in batch.")
    except Exception as e:
        if debug:
            print(f"Error inserting batch: {e}")
    finally:
        conn.close()


def update_markdown_text_row(
    db_path: Union[str, Path],
    id: int,
    markdown_text: str,
    markdown_text_tokens: int,
    debug: bool = False,
):
    """
    Update an existing row in the SQLite database with new markdown text and
    token count.

    Args:
        db_path (Union[str, Path]): Path to the SQLite database file.
        id (int): Unique identifier for the row to update.
        markdown_text (str): Updated Markdown text.
        markdown_text_tokens (int): Updated token count for the Markdown text.
        debug (bool): If True, print debug messages (default: False).
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE articles
        SET markdown_text = ?, markdown_text_tokens = ?
        WHERE id = ?
        """,
        (markdown_text, markdown_text_tokens, id),
    )
    if cursor.rowcount > 0:
        if debug:
            print(f"Row with id {id} updated successfully.")
    else:
        if debug:
            print(f"No row found with id {id}. No changes were made.")
    conn.commit()
    conn.close()


def update_llm_cleaned_row(
    db_path: Union[str, Path],
    id: int,
    model: str,
    llm_cleaned_text: str,
    llm_cleaned_text_tokens: int,
    debug: bool = False,
):
    """
    Update an existing row in the SQLite database with new LLM-cleaned text and
    related fields.

    Args:
        db_path (Union[str, Path]): Path to the SQLite database file.
        id (int): Unique identifier for the row to update.
        model (str): Name of the model used for cleaning.
        llm_cleaned_text (str): Updated cleaned text after LLM processing.
        llm_cleaned_text_tokens (int): Updated token count for the cleaned text.
        debug (bool): If True, print debug messages (default: False).
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE articles
        SET model = ?, llm_cleaned_text = ?, llm_cleaned_text_tokens = ?
        WHERE id = ?
        """,
        (model, llm_cleaned_text, llm_cleaned_text_tokens, id),
    )
    if cursor.rowcount > 0:
        if debug:
            print(f"Row with id {id} updated successfully with LLM-cleaned data.")
    else:
        if debug:
            print(f"No row found with id {id}. No changes were made.")
    conn.commit()
    conn.close()
