"""
Helper functions for the project
"""

import logging
from pathlib import Path


logger = logging.getLogger("helpers")


def get_project_root():
    """Function that returns the project root directory."""

    current = Path.cwd()
    indicators = ["pyproject.toml"]

    for parent in [current] + list(current.parents):
        if any((parent / indicator).exists() for indicator in indicators):
            return parent


def ensure_directory(path: Path) -> None:
    """
    Ensure that a directory exists; if not, create it.

    Args:
        path (str): The directory path to ensure.

    Returns:
        path (Path): The ensured directory path.
    """

    Path(path).mkdir(parents=True, exist_ok=True)
    logger.debug("Directory ensured: %s", path)
