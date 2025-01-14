import os
from os import getenv
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv
from transformers import AutoTokenizer

from src.download_wiki_file import download_wiki_dump
from src.parse_wiki_file import process_articles_to_parquet
from src.utils.database import filter_rows_in_db, initialize_db
from src.utils.parallel import parallel_markdown_formatting

# Load environment variables (e.g., API keys)
load_dotenv()


def _load_yaml(path):
    with open(path) as file:
        return yaml.safe_load(file)


def run():
    base_path = Path(".")
    config = _load_yaml(base_path / "run_config.yaml")
    parsed_file_path = base_path / config["parsed_folder"] / config["parsed_file"]
    db_path = base_path / config["data_folder"] / config["db_file"]

    # 1 - Download Wiki dump file
    print("Downloading Wiki dump...")
    file_path, metadata_file_path = download_wiki_dump(
        folder_path=base_path / config["raw_folder"],
        file_name=config["raw_file"],  # output name
        metadata_file_name=config["raw_metadata_file"],
        url=(
            f"https://dumps.wikimedia.org/{config['domain_download']}/latest/"
            f"{config['domain_download']}-latest-pages-articles-multistream.xml.bz2"
        ),
    )
    print(f"Downloaded file: {file_path}")
    print(f"Metadata file: {metadata_file_path}")

    # 2 - Parse wiki dump file to generate Parquet
    print("\nParsing Wiki dump...")
    process_articles_to_parquet(
        input_path=file_path,
        output_path=parsed_file_path,
        domain=config["domain"],
        clean_text=True,
    )

    # 3 - Load articles
    print("\nLoading parsed articles...")
    data = pd.read_parquet(parsed_file_path)

    # 4 - Initialize DB (for storing formatted articles)
    print("\nInitializing SQLite DB...")
    initialize_db(db_path)

    # 5 - Filter existing articles
    print("Filtering articles already in DB...")
    filtered_data = filter_rows_in_db(
        df=data,
        db_path=db_path,
        id_column="id",
    )

    # 5.1 - (EXTRA) Select only a handful of rows to test the pipeline
    filtered_data = filtered_data.head(50)

    # 6 - Format articles in Markdown
    print("Formatting articles in Markdown...")
    huggingface_token = getenv("HUGGINGFACE_TOKEN")
    tokenizer = AutoTokenizer.from_pretrained(
        config["model_hf"], token=huggingface_token
    )

    prompts = _load_yaml(base_path / "prompts.yaml")

    # Disable tokenizer parallelism (going to be called withing ThreadPool)
    os.environ["TOKENIZERS_PARALLELISM"] = "false"

    parallel_markdown_formatting(
        data=filtered_data,
        domain=config["domain"],
        model_openrouter=config["model_openrouter"],
        template=prompts["format_markdown"],
        tokenizer=tokenizer,
        model_hf=config["model_hf"],
        db_path=db_path,
        max_tokens=config["max_tokens"],
        max_workers=os.cpu_count() * 5,
    )


if __name__ == "__main__":
    run()
