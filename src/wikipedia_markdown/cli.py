import os
import click
from pathlib import Path
from dotenv import load_dotenv
from transformers import AutoTokenizer

from wikipedia_markdown.utils.yaml import load_yaml
from wikipedia_markdown.download_wiki_file import download_wiki_dump
from wikipedia_markdown.parse_wiki_file import parse_articles
from wikipedia_markdown.format_markdown import format_articles
from wikipedia_markdown.clean_markdown import clean_articles

# Load Local environment variables
# - OpenRouter API Key
# - HuggingFace API Key
load_dotenv()

@click.command()
def download():
    """Download the latest Wikipedia dump."""
    config_path = Path("run_config.yaml")
    config = load_yaml(config_path)

    data_folder = Path(config["data_folder"])
    raw_file = config["raw_file"]
    raw_metadata_file = config["raw_metadata_file"]
    domain_download = config["domain_download"]

    url = (
        f"https://dumps.wikimedia.org/{domain_download}/latest/"
        f"{domain_download}-latest-pages-articles-multistream.xml.bz2"
    )

    file_path, metadata_file_path = download_wiki_dump(
        folder_path=data_folder,
        file_name=raw_file,
        metadata_file_name=raw_metadata_file,
        url=url,
    )
    click.echo(f"Downloaded file: {file_path}")
    click.echo(f"Metadata saved in: {metadata_file_path}")


@click.command()
def parse():
    """Parse the downloaded Wikipedia dump and save articles to the database."""
    config_path = Path("run_config.yaml")
    config = load_yaml(config_path)

    data_folder = Path(config["data_folder"])
    file_path = data_folder / config["raw_file"]
    db_path = data_folder / config["db_file"]
    domain = config["domain"]
    huggingface_token = os.getenv("HUGGINGFACE_TOKEN")
    tokenizer = AutoTokenizer.from_pretrained(config["model_hf"], token=huggingface_token)

    # Parse all articles
    parse_articles(
        file_path=file_path,
        db_path=db_path,
        domain=domain,
        tokenizer=tokenizer,
        clean_text=True,
        max_workers=os.cpu_count() or 4,
    )
    click.echo("Wikipedia dump parsing completed.")
    click.echo(f"Database Path: {db_path}")


@click.option(
    "--max-workers",
    type=int,
    default=None,
    help="Maximum number of workers for parallel processing. Defaults to the number of CPU cores."
)
@click.option(
    "--batch-size",
    type=int,
    default=1000,
    help="Maximum number of rows to process. Defaults to 1000."
)
@click.command()
def format(max_workers, batch_size):
    """Format the markdown of all articles in the database."""
    config_path = Path("run_config.yaml")
    config = load_yaml(config_path)

    data_folder = Path(config["data_folder"])
    db_path = data_folder / config["db_file"]
    huggingface_token = os.getenv("HUGGINGFACE_TOKEN")
    tokenizer = AutoTokenizer.from_pretrained(config["model_hf"], token=huggingface_token)

    # If max_workers is not provided, default to the number of CPU cores
    if max_workers is None:
        max_workers = os.cpu_count() or 4

    # Format all articles
    format_articles(
        db_path=db_path,
        tokenizer=tokenizer,
        max_workers=8,
        batch_size=batch_size,
        debug=False,
    )
    click.echo("Markdown formatting completed.")
    click.echo(f"Database Path: {db_path}")


@click.command()
@click.option(
    "--max-workers",
    type=int,
    default=None,
    help="Maximum number of workers for parallel processing. Defaults to the number of CPU cores."
)
@click.option(
    "--max-rows",
    type=int,
    default=None,
    help="Maximum number of rows to process. Defaults to 1000."
)
def clean(max_workers, max_rows):
    """Clean the markdown text of articles in the database."""
    config_path = Path("run_config.yaml")
    config = load_yaml(config_path)

    # Disable tokenizer parallelism (going to be called withing ThreadPool)
    os.environ["TOKENIZERS_PARALLELISM"] = "false"

    data_folder = Path(config["data_folder"])
    db_path = data_folder / config["db_file"]
    model_openrouter = config["model_openrouter"]
    model_hf = config["model_hf"]
    huggingface_token = os.getenv("HUGGINGFACE_TOKEN")
    tokenizer = AutoTokenizer.from_pretrained(model_hf, token=huggingface_token)

    # Load the prompt template from the prompts.yaml file
    prompts_path = Path("prompts.yaml")
    prompts = load_yaml(prompts_path)
    prompt_template = prompts["clean_markdown"]

    # If max_workers is not provided, default to the number of CPU cores
    if max_workers is None:
        max_workers = os.cpu_count() or 4

    # Call the clean function
    clean_articles(
        db_path=db_path,
        model_openrouter=model_openrouter,
        tokenizer=tokenizer,
        prompt=prompt_template,
        max_workers=max_workers,
        max_tokens=5000,  # Token limit for clean_text vs clean_long_text
        max_rows=max_rows,
        debug=False,
    )

    click.echo("Markdown cleaning completed.")
    click.echo(f"Database Path: {db_path}")