import json
import os
import time
from os import getenv
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from dotenv import load_dotenv
from langchain_core.prompts import PromptTemplate
from openai import OpenAI
from openai.resources.batches import Batch
from transformers import AutoTokenizer

from src.wikipedia_markdown.utils.database import (
    get_ids_with_empty_llm_cleaned_text,
    get_rows_from_ids,
    update_llm_cleaned_row,
)
from wikipedia_markdown.utils.tokenizer import count_tokens
from wikipedia_markdown.utils.yaml import load_yaml


def process_batches(
    client: OpenAI,
    db_path: str,
    prompt: str,
    prompt_tokens: int,
    max_output_tokens: int,
    max_batch_tokens: int,
    results_path: Path,
    max_batches: Optional[int] = None,  # New optional parameter
) -> None:
    """
    Generate batches dynamically from the database and process them one by one.

    Args:
        client (OpenAI): The OpenAI client instance.
        db_path (str): Path to the SQLite database file.
        prompt (str): The prompt template to use for cleaning markdown.
        prompt_tokens (int): Number of tokens in the prompt.
        max_output_tokens (int): Maximum number of tokens for the output.
        max_batch_tokens (int): Maximum number of tokens per batch.
        results_path (Path): Directory to save batch job results.
        max_batches (Optional[int]): Maximum number of batches to process.
            If None, process all batches.
    """
    # Get IDs of rows with fewer than max_output_tokens tokens
    ids = get_ids_with_empty_llm_cleaned_text(
        db_path=db_path, max_markdown_tokens=max_output_tokens
    )

    if not ids:
        print("No rows to clean.")
        return

    # Fetch rows corresponding to the IDs
    rows = get_rows_from_ids(db_path, ids)

    # Convert rows to a DataFrame for easier manipulation
    df = pd.DataFrame(rows)

    # Prepare the prompt template
    prompt_template = PromptTemplate(template=prompt, input_variables=["text"])

    # Initialize variables for batching
    current_batch: List[Dict[str, Any]] = []
    current_batch_tokens = 0
    batch_number = 1
    batches_processed = 0  # Counter for the number of batches processed

    for _, row in df.iterrows():
        markdown_text = row["markdown_text"]
        markdown_tokens = row["markdown_text_tokens"]

        if (
            markdown_text and markdown_tokens
        ):  # Ensure the text and token count are valid
            content = prompt_template.format(text=markdown_text)
            input_data = {
                "custom_id": f"{row['id']}",  # Unique ID for each request
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": "gpt-4o-mini",  # Use the appropriate model
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a helpful assistant.",
                        },
                        {"role": "user", "content": content},
                    ],
                    "max_tokens": max_output_tokens,
                },
            }

            # Check if adding this row exceeds the max batch tokens
            total = current_batch_tokens + markdown_tokens + prompt_tokens
            if total > max_batch_tokens:
                # Process the current batch
                print(f"Processing batch {batch_number}")
                process(client, current_batch, batch_number, db_path, results_path)
                batch_number += 1
                batches_processed += 1

                # Check if the maximum number of batches has been processed
                if max_batches is not None and batches_processed >= max_batches:
                    print(f"Processed {max_batches} batches. Exiting.")
                    return

                # Start a new batch
                current_batch = []
                current_batch_tokens = 0

            # Add the row to the current batch
            current_batch.append(input_data)
            current_batch_tokens += markdown_tokens

    # Process the last batch if it's not empty
    if current_batch:
        print(f"Processing batch {batch_number}")
        process(client, current_batch, batch_number, db_path, results_path)
        batches_processed += 1

        # Check if the maximum number of batches has been processed
        if max_batches is not None and batches_processed >= max_batches:
            print(f"Processed {max_batches} batches. Exiting.")
            return


def process(
    client: OpenAI,
    batch: List[Dict[str, Any]],
    batch_number: int,
    db_path: str,
    results_path: Path,
) -> None:
    """
    Process a single batch by uploading it, creating a batch job, monitoring it,
    and updating the database with the results.

    Args:
        client (OpenAI): The OpenAI client instance.
        batch (List[Dict[str, Any]]): The batch data to process.
        batch_number (int): The batch number for logging purposes.
        db_path (str): Path to the SQLite database file.
        results_path (Path): Directory to save batch job results.
    """
    # Upload the batch
    file_id = upload_single_batch(client, batch)
    if not file_id:
        print(f"Failed to upload batch {batch_number}.")
        return

    print(f"Batch {batch_number} uploaded with ID: {file_id}")

    # Create a batch job for the uploaded file
    batch_job = create_batch_job(
        client=client,
        input_file_id=file_id,
        metadata={"description": f"Batch {batch_number}"},
    )
    if not batch_job:
        print(f"Failed to create a batch job for batch {batch_number}.")
        return

    print(f"Created batch job with ID: {batch_job.id}")

    # Monitor the batch job until it finishes
    monitor_batch_job(client, batch_job.id, db_path, results_path)

    print(f"Batch job {batch_job.id} finished with status: {batch_job.status}\n")


def upload_single_batch(client: OpenAI, batch: List[Dict[str, Any]]) -> Optional[str]:
    """
    Upload a single batch to OpenAI.

    Args:
        client (OpenAI): The OpenAI client instance.
        batch (List[Dict[str, Any]]): The batch data to upload.

    Returns:
        Optional[str]: The file ID returned by OpenAI, or None if the upload fails.
    """
    # Convert the batch to a JSONL string
    jsonl_data = "\n".join(json.dumps(item) for item in batch)

    # Upload the batch as a file
    batch_input_file = client.files.create(
        file=jsonl_data.encode("utf-8"),
        purpose="batch",
    )
    return batch_input_file.id


def create_batch_job(
    client: OpenAI, input_file_id: str, metadata: Optional[dict] = None
) -> Batch:
    """
    Create a batch job using the OpenAI Batch API.

    Args:
        client (OpenAI): The OpenAI client instance.
        input_file_id (str): The file ID of the uploaded batch.
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


def monitor_batch_job(client: OpenAI, batch_id: str, db_path: str, results_path: Path):
    """
    Monitor the status of a batch job until it is completed, then download the results
    and update the database.

    Args:
        client (OpenAI): The OpenAI client instance.
        batch_id (str): The ID of the batch job to monitor.
        db_path (str): Path to the SQLite database file.
        results_path (Path): Directory to save batch job results.
    """
    while True:
        batch = client.batches.retrieve(batch_id)
        print(f"Batch ID: {batch_id}")
        print(f"Status: {batch.status}")
        print(f"Counts: {batch.request_counts}")
        print("-" * 40)

        if batch.status == "completed":
            # Download the output file content
            file_response = client.files.content(batch.output_file_id)

            # Construct the path to save the result
            result_file_name = f"batch_result_{batch_id}.jsonl"
            result_file_path = results_path / result_file_name
            results_path.mkdir(parents=True, exist_ok=True)

            # Save the content to the file
            with open(result_file_path, "wb") as f:
                f.write(file_response.content)

            # Process the JSONL file and update the database
            process_jsonl_and_update_db(result_file_path, db_path)

            break

        if batch.status == "failed":
            raise Exception(f"Batch ID: {batch_id} failed.")

        # Wait for 5 seconds before checking again
        time.sleep(5)


def process_jsonl_and_update_db(
    jsonl_file_path: Path, db_path: str, debug: bool = False
):
    """
    Process a `.jsonl` file and update the database.

    Args:
        jsonl_file_path (Path): Path to the `.jsonl` file containing batch job results.
        db_path (str): Path to the SQLite database file.
        debug (bool): If True, print debug messages (default: False).
    """
    with open(jsonl_file_path, "r", encoding="utf-8") as file:
        for line in file:
            # Parse each line as a JSON object
            row = json.loads(line)

            # Extract the required data
            custom_id = int(row["custom_id"])  # The ID of the row in the database
            response_body = row["response"]["body"]
            assistant_content = response_body["choices"][0]["message"][
                "content"
            ]  # Assistant's response
            completion_tokens = response_body["usage"][
                "completion_tokens"
            ]  # Token count
            model = response_body["model"]  # Model used for processing

            # Update the database row
            update_llm_cleaned_row(
                db_path=db_path,
                id=custom_id,
                model=model,
                llm_cleaned_text=assistant_content,
                llm_cleaned_text_tokens=completion_tokens,
                debug=debug,
            )


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

    # Load the prompt template
    prompts = load_yaml(base_path / "prompts.yaml")
    prompt = prompts["clean_markdown"]

    # Set the results directory
    results_path = base_path / config["openai_batch_job_results_path"]
    results_path.mkdir(parents=True, exist_ok=True)

    # Estimate number of tokens
    # Load Local environment variables (OpenRouter API Key)
    load_dotenv()
    huggingface_token = getenv("HUGGINGFACE_TOKEN")
    # Disable tokenizer parallelism
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    tokenizer = AutoTokenizer.from_pretrained("Xenova/gpt-4o", token=huggingface_token)

    # Process batches dynamically
    process_batches(
        client=client,
        db_path=db_path,
        prompt=prompt,
        prompt_tokens=count_tokens(tokenizer, prompt),
        max_output_tokens=config["max_tokens"],
        max_batch_tokens=config["openai_batch_tokens"],
        results_path=results_path,
        max_batches=3,
    )
