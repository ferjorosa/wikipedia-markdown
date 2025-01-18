import json
from datetime import datetime
from pathlib import Path
from typing import Tuple

import requests
from tqdm import tqdm


def download_wiki_dump(
    folder_path: Path,
    file_name: str = "simplewiki-latest-pages-articles-multistream.xml.bz2",
    metadata_file_name: str = "download_metadata.json",
    url: str = (
        "https://dumps.wikimedia.org/simplewiki/latest/"
        "simplewiki-latest-pages-articles-multistream.xml.bz2"
    ),
) -> Tuple[Path, Path]:
    """
    Downloads the latest dump from Simple Wikipedia and retrieves the associated date.

    Args:
        folder_path (Path): The folder where the files will be saved.
        file_name (str): The name of the file to save the downloaded dump. Defaults
            to the standard dump file name.
        metadata_file_name (str): The name of the file to save metadata. Defaults to
            "download_metadata.json".

    Returns:
        Tuple[Path, Path]:
            - Path to the downloaded file.
            - Path to the metadata file.

    Raises:
        Exception: If the URL is inaccessible, the last modified date is missing, or
            the download fails.
    """
    folder_path = Path(folder_path)
    folder_path.mkdir(parents=True, exist_ok=True)

    # Full paths for the dump file and metadata file
    file_path = folder_path / file_name
    metadata_file_path = folder_path / metadata_file_name

    # Get metadata from the server
    try:
        response = requests.head(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
    except requests.RequestException as e:
        raise Exception(f"Failed to access URL: {e}")

    last_modified = response.headers.get("Last-Modified")
    if not last_modified:
        raise Exception("Could not retrieve the last modified date from headers.")

    dump_date = datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")
    formatted_date = dump_date.strftime("%Y-%m-%d")

    # Download the file
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
    except requests.RequestException as e:
        raise Exception(f"Failed to download file: {e}")

    total_size = int(response.headers.get("Content-Length", 0))

    with open(file_path, "wb") as file:
        with tqdm(total=total_size, unit="B", unit_scale=True, desc=file_name) as pbar:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)
                    pbar.update(len(chunk))

    # Write metadata to a JSON file
    metadata = {
        "file_name": file_name,
        "download_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "last_modified_date": formatted_date,
        "file_size": total_size,
        "downloaded_file_path": str(file_path),
    }

    with open(metadata_file_path, "w") as f:
        json.dump(metadata, f, indent=4)

    return file_path, metadata_file_path


if __name__ == "__main__":
    file_path, metadata_file_path = download_wiki_dump(
        folder_path=Path("../data/raw"), file_name="articles.xml.bz2"
    )
    print(f"Downloaded file: {file_path}")
    print(f"Metadata saved in: {metadata_file_path}")
