import json
from os import getenv
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from openai import OpenAI
from openai.resources.batches import Batch

from wikipedia_markdown.utils.yaml import load_yaml


def upload_single_batch(client: OpenAI, file_path: str) -> Optional[str]:
    """
    Upload a single batch file to OpenAI.

    Args:
        client (OpenAI): The OpenAI client instance.
        file_path (str): Path to the `.jsonl` file to upload.

    Returns:
        Optional[str]: The file ID returned by OpenAI, or None if the upload fails.
    """
    with open(file_path, "rb") as file:
        batch_input_file = client.files.create(file=file, purpose="batch")
    return batch_input_file.id


def create_batch_job(
    client: OpenAI, input_file_id: str, metadata: Optional[dict] = None
) -> Batch:
    """
    Create a batch job using the OpenAI Batch API.

    Args:
        client (OpenAI): The OpenAI client instance.
        input_file_id (str): The file ID of the uploaded `.jsonl` file.
        metadata (Optional[dict]): Metadata to attach to the batch job.

    Returns:
        Optional[dict]: The batch job details, or None if the creation fails.
    """
    return client.batches.create(
        input_file_id=input_file_id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata=metadata or {"description": "Batch job for cleaning markdown text"},
    )


def save_batch_job_info(batch_job: Batch, file_path: Path):
    """
    Save the batch job result as a `.json` file.

    Args:
        batch_job (Batch): The batch job details returned by OpenAI.
    """
    # Convert the batch_job object to a dictionary using model_dump()
    batch_job_dict = batch_job.model_dump()

    # Save the dictionary as JSON
    with open(file_path, "w") as f:
        json.dump(batch_job_dict, f, indent=4)
    print(f"Saved batch job result to {file_path}")


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

    # Construct the full path to the database file
    data_folder = base_path / config["data_folder"]
    db_file = config["db_file"]
    db_path = data_folder / db_file

    prompts = load_yaml(base_path / "prompts.yaml")

    # Get the output path from the config
    batch_files_path = base_path / config["openai_batch_job_files_path"]

    job_file_name = "batch_input_1.jsonl"
    job_file_path = batch_files_path / job_file_name

    job_info_file_name = (
        Path(job_file_name)
        .with_stem(Path(job_file_name).stem + "_info")
        .with_suffix(".json")
    )
    job_info_file_path = batch_files_path / job_info_file_name

    # Upload the batch file
    file_id = upload_single_batch(client, job_file_path)
    if file_id:
        print(f"Batch file uploaded with ID: {file_id}")

        # Create a batch job for the uploaded file
        batch_job = create_batch_job(
            client=client,
            input_file_id=file_id,
            metadata={"description": job_file_name},
        )
        if batch_job:
            print(f"Created batch job with ID: {batch_job.id}")

            save_batch_job_info(
                batch_job=batch_job,
                file_path=job_info_file_path,
            )

    else:
        print("Failed to upload the batch file.")
