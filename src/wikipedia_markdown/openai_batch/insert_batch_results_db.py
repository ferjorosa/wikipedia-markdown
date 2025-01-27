import json
from pathlib import Path

from src.wikipedia_markdown.utils.database import update_llm_cleaned_row
from src.wikipedia_markdown.utils.yaml import load_yaml


def process_jsonl_and_update_db(
    jsonl_file_path: Path, db_path: Path, debug: bool = False
):
    """
    Process a `.jsonl` file and update the database.

    Args:
        jsonl_file_path (Path): Path to the `.jsonl` file containing batch job results.
        db_path (Path): Path to the SQLite database file.
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
    # Set the base path
    base_path = Path("../../../")

    # Load the YAML configuration
    config_path = base_path / "config.yaml"
    config = load_yaml(config_path)

    # Construct the full path to the database file
    data_folder = base_path / config["data_folder"]
    db_file = config["db_file"]
    db_path = data_folder / db_file

    # Path to the `.jsonl` file containing batch job results
    batch_files_path = base_path / config["openai_batch_job_results_path"]
    result_file_name = "batch_result_batch_6795146d7d00819082a8ba4463cabf73.jsonl"
    result_file_path = batch_files_path / result_file_name

    # Process the `.jsonl` file and update the database
    process_jsonl_and_update_db(
        jsonl_file_path=result_file_path,
        db_path=db_path,
        debug=True,  # Set to True to print debug messages
    )
