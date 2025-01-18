import click
from pathlib import Path
import yaml
from src.download_wiki_file import download_wiki_dump

def load_config(config_path: Path) -> dict:
    """Load configuration from a YAML file."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config

@click.command()
def download():
    """Download the latest Wikipedia dump."""
    config_path = Path("run_config.yaml")
    config = load_config(config_path)

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

if __name__ == "__main__":
    download()