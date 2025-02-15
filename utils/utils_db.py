import sqlite3
import json
import pathlib

def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """Inserts the message into the SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create the table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS streamed_messages (
                message TEXT,
                author TEXT,
                timestamp TEXT,
                category TEXT,
                sentiment REAL,
                keyword_mentioned TEXT,
                message_length INTEGER
            )
        """)

        # Insert the message into the database
        cursor.execute("""
            INSERT INTO streamed_messages (message, author, timestamp, category, sentiment, keyword_mentioned, message_length)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            message['message'],
            message['author'],
            message['timestamp'],
            message['category'],
            message['sentiment'],
            message['keyword_mentioned'],
            message['message_length']
        ))

        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        print(f"Error inserting message into SQLite database: {e}")
