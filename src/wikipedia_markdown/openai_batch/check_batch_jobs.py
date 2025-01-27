import json
from os import getenv
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv
from openai import OpenAI

from wikipedia_markdown.utils.yaml import load_yaml


def get_batch_info_files(directory: Path) -> List[Path]:
    """
    Get a list of `.json` batch info files in the specified directory.

    Args:
        directory (Path): The directory to search for `.json` files.

    Returns:
        List[Path]: A list of paths to `.json` batch info files.
    """
    return list(directory.glob("*_info.json"))


def get_batch_id_from_info_file(info_file: Path) -> Optional[str]:
    """
    Extract the batch ID from a batch info `.json` file.

    Args:
        info_file (Path): The path to the batch info `.json` file.

    Returns:
        Optional[str]: The batch ID, or None if the file is invalid.
    """
    try:
        with open(info_file, "r") as f:
            batch_info = json.load(f)
            return batch_info.get("id")
    except (json.JSONDecodeError, FileNotFoundError):
        return None


if __name__ == "__main__":
    # Load Local environment variables (OpenAI API Key)
    load_dotenv()
    openai_token = getenv("OPENAI_TOKEN")

    # Initialize the OpenAI client
    client = OpenAI(api_key=openai_token)

    # Set the base path
    base_path = Path("../../../")

    # Load the YAML configuration
    config_path = base_path / "config.yaml"
    config = load_yaml(config_path)

    # Set the directory containing batch info files
    batch_files_path = base_path / config["openai_batch_job_files_path"]

    # Get all batch info files and sort them by name
    batch_info_files = sorted(
        get_batch_info_files(batch_files_path), key=lambda x: x.name
    )

    # Process each batch info file
    for info_file in batch_info_files:
        # Extract the batch ID from the info file
        batch_id = get_batch_id_from_info_file(info_file)
        if not batch_id:
            print(f"Skipping invalid or missing batch ID in file: {info_file.name}")
            continue

        # Retrieve and print the batch job status
        try:
            batch = client.batches.retrieve(batch_id)
            print(f"File: {info_file}")
            print(f"Batch ID: {batch_id}")
            print(f"Status: {batch.status}")
            print(f"Counts: {batch.request_counts}")
            print("-" * 40)
        except Exception as e:
            print(f"Failed to retrieve batch {batch_id}: {e}")
