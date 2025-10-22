"""
Script to test postgres db connection
"""

import os
from dotenv import load_dotenv

import psycopg2

load_dotenv()

POSTGRES_DB_HOST = os.getenv("POSTGRES_DB_HOST")
POSTGRES_DB_PORT = os.getenv("POSTGRES_DB_PORT")
POSTGRES_DB_NAME = os.getenv("POSTGRES_DB_NAME")
POSTGRES_DB_USER = os.getenv("POSTGRES_DB_USER")
DB_PASS = os.getenv("DB_PASS")
POSTGRES_DB_URL = os.getenv("DATABASE_URL")


def test_db_connection():
    """Test PostgreSQL connection"""

    try:
        conn = psycopg2.connect(POSTGRES_DB_URL)

        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()

        print("✅ Database connection successful!")
        print(f"PostgreSQL version: {db_version[0]}")

        cursor.close()
        conn.close()

    except Exception as e:  # pylint: disable=broad-except
        print(f"❌ Database connection failed: {e}")


if __name__ == "__main__":
    test_db_connection()
