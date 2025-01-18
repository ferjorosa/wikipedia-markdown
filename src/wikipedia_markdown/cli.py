import os
import click
from pathlib import Path
from transformers import AutoTokenizer

from wikipedia_markdown.utils.yaml import load_yaml
from wikipedia_markdown.download_wiki_file import download_wiki_dump
from wikipedia_markdown.parse_wiki_file import parse_all_articles
from wikipedia_markdown.format_markdown import format_all_articles


@click.command()
def download_wiki_dump():
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
def parse_wiki_dump():
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
    parse_all_articles(
        file_path=file_path,
        db_path=db_path,
        domain=domain,
        tokenizer=tokenizer,
        clean_text=True,
        max_workers=os.cpu_count() or 4,
    )
    click.echo("Wikipedia dump parsing completed.")
    click.echo(f"Database Path: {db_path}")


@click.command()
def format_articles():
    """Format the markdown of all articles in the database."""
    config_path = Path("run_config.yaml")
    config = load_yaml(config_path)

    data_folder = Path(config["data_folder"])
    db_path = data_folder / config["db_file"]
    huggingface_token = os.getenv("HUGGINGFACE_TOKEN")
    tokenizer = AutoTokenizer.from_pretrained(config["model_hf"], token=huggingface_token)

    # Format all articles
    format_all_articles(
        db_path=db_path,
        tokenizer=tokenizer,
        max_workers=8,
        batch_size=1000,
        debug=True,
    )
    click.echo("Markdown formatting completed.")
    click.echo(f"Database Path: {db_path}")


