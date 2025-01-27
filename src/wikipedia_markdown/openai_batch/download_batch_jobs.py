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

    # Set the directory to save batch results
    results_path = base_path / config["openai_batch_job_results_path"]
    results_path.mkdir(parents=True, exist_ok=True)

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

        # Retrieve the batch job details
        try:
            batch = client.batches.retrieve(batch_id)
            if not batch.output_file_id:
                print(f"No output file found for batch {batch_id}. Skipping.")
                continue

            # Download the output file content
            file_response = client.files.content(batch.output_file_id)

            # Construct the path to save the result
            result_file_name = f"batch_result_{batch_id}.jsonl"
            result_file_path = results_path / result_file_name

            # Save the content to the file
            with open(result_file_path, "wb") as f:
                f.write(file_response.content)

            print(f"Batch job result saved to {result_file_path}")

        except Exception as e:
            print(f"Failed to retrieve or save results for batch {batch_id}: {e}")
