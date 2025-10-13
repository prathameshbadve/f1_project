"""
Test the working of the FastF1 Client
"""

import sys
from pathlib import Path

current = Path.cwd()
indicators = ["pyproject.toml"]

for parent in [current] + list(current.parents):
    if any((parent / indicator).exists() for indicator in indicators):
        project_root = parent
        break
else:
    raise RuntimeError("Project root with required indicators not found.")

sys.path.insert(0, str(project_root))

# pylint: disable=wrong-import-position

from config.logging import setup_logging  # noqa: E402
from src.data_ingestion.fastf1_client import FastF1Client  # noqa: E402

setup_logging()
client = FastF1Client()

# Get season schedule
print("\n" + "=" * 60)
print("TEST 1: Fetching 2024 Season Schedule")
print("=" * 60)
schedule = client.get_season_schedule(2024)

# Get a session object and print event details
print("\n" + "=" * 60)
print("TEST 2: Fetching 2024 Italian Grand Prix Session")
print("=" * 60)
italy_2024 = client.get_session(2024, "Italian Grand Prix", "R")
session_info = italy_2024.session_info
print(session_info)

print("\n" + "=" * 60)
print("All tests completed!")
