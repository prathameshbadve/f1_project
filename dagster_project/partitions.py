"""
Dagster partitions for F1 data pipeline.

Partitions allow us to handle many similar data products (e.g., all race sessions)
with a single asset definition instead of creating hundreds of separate assets.
"""

from typing import List
from dagster import StaticPartitionsDefinition  # , DynamicPartitionsDefinition

from src.data_ingestion.schedule_loader import ScheduleLoader


def get_2024_session_partitions() -> List[str]:
    """
    Get all session partitions for 2024 season.

    Returns list of partition keys in format: "event_name|session_type"
    Example: "bahrain_grand_prix|R", "bahrain_grand_prix|Q"
    """

    schedule_loader = ScheduleLoader()

    # Load 2024 season schedule
    schedule = schedule_loader.load_season_schedule(2024, force_refresh=False)

    prtn_keys = []

    for _, event_row in schedule.iterrows():
        event_name = event_row["EventName"]

        # Get all sessions for this event (Session1, Session2, etc.)
        sessions = []
        for i in range(1, 6):  # Session1 through Session5
            session_col = f"Session{i}"
            if session_col in event_row and event_row[session_col]:
                session_type = event_row[session_col]
                sessions.append(session_type)

        # Create partition key for each session
        for session_type in sessions:
            # Use pipe separator: "event_name|session_type"
            partition_key = f"{event_name}|{session_type}"
            prtn_keys.append(partition_key)

    return prtn_keys


# ============================================================================
# STATIC PARTITIONS (2024 Season)
# ============================================================================

# Create the static partitions definition for 2024
# This is evaluated once when Dagster loads, so it's fast
try:
    partition_keys = get_2024_session_partitions()

    f1_2024_sessions_partitions = StaticPartitionsDefinition(
        partition_keys=partition_keys
    )

    # For logging/debugging
    print(f"✅ Created {len(partition_keys)} partitions for 2024 season")
    print(f"   Sample partitions: {partition_keys[:3]}")

except Exception as e:  # pylint: disable=broad-except
    print(f"⚠️  Failed to create partitions: {e}")
    print("   Creating empty partitions as fallback")

    # Fallback: empty partitions (Dagster won't crash)
    f1_2024_sessions_partitions = StaticPartitionsDefinition(partition_keys=[])


# ============================================================================
# DYNAMIC PARTITIONS (Any Year/Event/Session)
# ============================================================================

# def get_season_session_partitions(year: int) -> List[str]:
#     """
#     Get all session partitions for a given year.

#     Args:
#         year: Season year (e.g., 2024, 2023, 2022)

#     Returns:
#         List of partition keys in format: "event_name|session_type"
#     """

#     schedule_loader = ScheduleLoader()

#     # Load season schedule
#     schedule = schedule_loader.load_season_schedule(year, force_refresh=False)

#     prtn_keys = []

#     for _, event_row in schedule.iterrows():
#         event_name = event_row["EventName"]

#         # Get all sessions for this event
#         sessions = []
#         for i in range(1, 6):  # Session1 through Session5
#             session_col = f"Session{i}"
#             if session_col in event_row and event_row[session_col]:
#                 session_type = event_row[session_col]
#                 sessions.append(session_type)

#         # Create partition key for each session
#         for session_type in sessions:
#             partition_key = f"{event_name}|{session_type}"
#             prtn_keys.append(partition_key)

#     return prtn_keys

# Dynamic partitions that can be populated at runtime
# f1_dynamic_sessions_partitions = DynamicPartitionsDefinition(name="f1_dynamic_sessions")
