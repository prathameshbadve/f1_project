"""
Dagster sensors for F1 data pipeline
"""

from dagster_project.sensors.raw_data_sensor import raw_data_change_sensor

all_sensors = [
    raw_data_change_sensor,
]
