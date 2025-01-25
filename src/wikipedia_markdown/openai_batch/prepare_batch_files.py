import json
from pathlib import Path
from typing import List

from langchain_core.prompts import PromptTemplate

from src.wikipedia_markdown.utils.database import (
    get_ids_with_empty_llm_cleaned_text,
    get_rows_from_ids,
)
from wikipedia_markdown.utils.yaml import load_yaml


def create_jsonl_files(
    db_path: str,
    prompt: str,
    max_output_tokens: int,
    output_path: str,
    batch_size: int = 50000,
) -> List[str]:
    """
    Create `.jsonl` files for the OpenAI Batch API.

    Args:
        db_path (str): Path to the SQLite database file.
        prompt (str): The prompt template to use for cleaning markdown.
        max_output_tokens (int): Maximum number of tokens for the output.
        output_path (str): Directory where `.jsonl` files will be saved.
        batch_size (int): Maximum number of rows per `.jsonl` file.

    Returns:
        List[str]: List of file paths for the created `.jsonl` files.
    """
    # Ensure the output directory exists
    output_dir = Path(output_path)
    if not output_dir.exists():
        print(f"Creating output directory: {output_path}")
        output_dir.mkdir(parents=True, exist_ok=True)

    # Get IDs of articles that need cleaning
    ids = get_ids_with_empty_llm_cleaned_text(
        db_path=db_path, max_markdown_tokens=max_output_tokens
    )

    if not ids:
        print("No rows to clean.")
        return []

    # Fetch rows corresponding to the IDs
    rows = get_rows_from_ids(db_path, ids)

    prompt_template = PromptTemplate(template=prompt, input_variables=["text"])

    # Prepare the batch inputs
    batch_inputs = []
    for row in rows:
        markdown_text = row["markdown_text"]
        if markdown_text:  # Ensure the text is not empty
            content = prompt_template.format(text=markdown_text)
            batch_inputs.append(
                {
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
            )

    # Split the inputs into chunks of `batch_size`
    file_paths = []
    for i in range(0, len(batch_inputs), batch_size):
        chunk = batch_inputs[i : i + batch_size]
        file_name = f"batch_input_{i // batch_size + 1}.jsonl"
        file_path = output_dir / file_name
        with open(file_path, "w") as f:
            for input_data in chunk:
                # Write each JSON object as a single line
                f.write(json.dumps(input_data) + "\n")
        file_paths.append(str(file_path))
        print(f"Created {file_path} with {len(chunk)} requests.")

    return file_paths


if __name__ == "__main__":
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
    output_path = base_path / config["openai_batch_job_files_path"]

    # Create `.jsonl` files
    file_paths = create_jsonl_files(
        db_path=db_path,
        prompt=prompts["clean_markdown"],
        max_output_tokens=config["max_tokens"],
        output_path=output_path,
        batch_size=config["openai_batch_job_size"],
    )

    print(f"Generated .jsonl files: {file_paths}")
