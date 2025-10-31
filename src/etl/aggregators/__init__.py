"""
ETL Aggregators

Data aggregators that combine raw race data into unified training datasets.
"""

from .base_aggregator import BaseAggregator, AggregationResult
from .results_aggregator import RaceResultsAggregator

__all__ = [
    "BaseAggregator",
    "AggregationResult",
    "RaceResultsAggregator",
]
