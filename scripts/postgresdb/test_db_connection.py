"""
Script to test postgres db connection
"""

import os
from dotenv import load_dotenv

import psycopg2

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_URL = os.getenv("DATABASE_URL")


def test_db_connection():
    """Test PostgreSQL connection"""

    try:
        conn = psycopg2.connect(DB_URL)

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
