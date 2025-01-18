import yaml
from pathlib import Path
from typing import Any, Dict


def load_yaml(path: Path) -> Dict[str, Any]:
    """Load and parse a YAML file into a Python dictionary.

    Args:
        path (Path): The path to the YAML file.

    Returns:
        Dict[str, Any]: A dictionary containing the parsed YAML data.
    """
    with open(path, "r") as file:
        return yaml.safe_load(file)