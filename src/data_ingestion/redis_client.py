"""
Client for interacting with Redis Cache
"""

import io
from datetime import timedelta
from typing import Dict, Optional

import pandas as pd
from redis import Redis

from config.settings import redis_config, RedisConfig
from config.logging import get_logger


class RedisClient:
    """
    Client for interacting with Redis Cache
    """

    def __init__(
        self,
        redis_cfg: RedisConfig = None,
    ):
        self.config = redis_cfg or redis_config
        self.logger = get_logger("data_ingestion.redis_client")
        self.cache_key_prefix = "f1:schedule:"
        self.ttl = timedelta(hours=24)  # Cache for 24 hours

        try:
            self.client = Redis(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            self.logger.info("Successfully initialized redis client")

        except Exception as e:
            self.logger.error("Failed to initialize redis client: %s", str(e))
            self.client = None
            raise

    def connect(self):
        """Manual connect to redis client"""

        if self.client is None:
            self.client = Redis(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
            self.logger.info("Successfully initialized redis client (Manually)")
            return True

        self.logger.warning("Already connected to redis client")
        return False

    def disconnect(self):
        """Close Redis connection"""

        if self.client:
            self.client.close()
            self.client = None

    def get_schedule(self, year: int) -> Optional[pd.DataFrame]:
        """
        Retrieve schedule from cache

        Args:
            year: Season year

        Returns:
            Schedule dictionary or None if not cached
        """
        cache_key = f"{self.cache_key_prefix}{year}"

        try:
            # Get from Redis
            parquet_bytes = self.client.get(cache_key)

            if parquet_bytes is None:
                self.logger.info("Schedule cache MISS for %d", year)
                return None

            # Convert back to DataFrame
            buffer = io.BytesIO(parquet_bytes)
            df = pd.read_parquet(buffer, engine="pyarrow")

            self.logger.info("Schedule cache HIT for %d (%d rows)", year, len(df))

            return df

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Error retrieving schedule from cache: %s", str(e))
            return None

    def set_schedule(self, year: int, df: pd.DataFrame) -> bool:
        """
        Cache DataFrame in Redis

        Args:
            df: DataFrame to cache
            year: Season year
            ttl: Custom TTL in seconds (optional)
        """

        if df is None or df.empty:
            self.logger.warning("Cannot cache empty DataFrame")
            return False

        try:
            cache_key = f"{self.cache_key_prefix}{year}"
            # Convert to Parquet bytes
            buffer = io.BytesIO()
            df.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
            parquet_bytes = buffer.getvalue()

            # Store in Redis
            self.client.setex(cache_key, self.ttl, parquet_bytes)

            size_mb = len(parquet_bytes) / (1024 * 1024)
            self.logger.info(
                "✅ Cached schedule for %d (%d rows, %2f MB, TTL: %d s)",
                year,
                len(df),
                size_mb,
                self.ttl,
            )
            return True

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("❌ Failed to cache DataFrame: %s", str(e))
            return False

    def delete_schedule(self, year: int) -> bool:
        """Delete schedule from cache"""

        try:
            cache_key = f"{self.cache_key_prefix}{year}"
            result = self.client.delete(cache_key)
            self.logger.info("Deleted cache: %s", cache_key)
            return result > 0

        except Exception as e:  # pylint: disable=broad-except
            self.logger.error("Failed to delete cache: %s", str(e))
            return False

    def get_cache_info(self) -> Dict:
        """Get information about cached items"""

        pattern = f"{self.cache_key_prefix}*"
        keys = self.client.keys(pattern)

        total_size = 0
        for key in keys:
            try:
                total_size += len(self.client.get(key))
            except Exception as e:  # pylint: disable=broad-except
                self.logger.warning("Failed to get size for key %s: %s", key, str(e))

        return {
            "total_keys": len(keys),
            "total_size_mb": total_size / (1024 * 1024),
            "keys": [k.decode() if isinstance(k, bytes) else k for k in keys],
        }
