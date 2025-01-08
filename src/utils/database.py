import sqlite3
from pathlib import Path
from typing import Union

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
                markdown_text TEXT,
                raw_text_tokens INTEGER,
                markdown_text_tokens INTEGER,
                model TEXT
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
    markdown_text: str,
    raw_text_tokens: int,
    markdown_text_tokens: int,
    model: str,
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
        markdown_text (str): Transformed Markdown text.
        raw_text_tokens (int): Token count of raw text.
        markdown_text_tokens (int): Token count of Markdown text.
        model (str): Name of the model.
        debug (bool): If True, print debug messages (default: False).
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT OR IGNORE INTO articles (
            id, title, url, raw_text, markdown_text,
            raw_text_tokens, markdown_text_tokens, model
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            id,
            title,
            url,
            raw_text,
            markdown_text,
            raw_text_tokens,
            markdown_text_tokens,
            model,
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
