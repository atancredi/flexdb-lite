import sqlite3
import json
import re
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class DocumentDB(ABC):
    """Abstract interface for a document-based database."""

    @abstractmethod
    def create_index(self, json_key: str, unique: bool = False):
        pass

    @abstractmethod
    def insert(self, data_dict: dict) -> Optional[int]:
        pass

    @abstractmethod
    def select(self, key: str, value: Any, op: str = "=") -> List[Dict]:
        pass

    @abstractmethod
    def update_field(self, record_id: int, key: str, new_value: Any):
        pass

    @abstractmethod
    def update_row(self, record_id: int, data_dict: dict):
        pass

    @abstractmethod
    def delete(self, record_id: int) -> bool:
        pass

    @abstractmethod
    def get_all(self) -> List[Dict]:
        pass


class FlexDB(DocumentDB):
    # Whitelist operators to prevent SQL injection in the operator field
    ALLOWED_OPERATORS = {"=", "!=", ">", "<", ">=", "<=", "LIKE"}

    def __init__(self, db_name: str, collection_name: str = "default_collection"):
        if not re.match(r"^[a-zA-Z0-9_]+$", collection_name):
            raise ValueError(f"Invalid collection name: {collection_name}")

        self.collection_name = collection_name
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.cursor.execute("PRAGMA journal_mode=WAL;")
        self._create_table()

    def _create_table(self):
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.collection_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                json_data TEXT CHECK(json_valid(json_data)),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        self.cursor.execute(query)
        self.conn.commit()
        return self

    def create_index(self, json_key: str, unique: bool = False):
        if not re.match(r"^[a-zA-Z0-9_]+$", json_key):
            raise ValueError("Invalid JSON key for indexing.")

        index_type = "UNIQUE " if unique else ""
        index_name = f"idx_{self.collection_name}_{json_key}"

        query = f"""
            CREATE {index_type}INDEX IF NOT EXISTS {index_name} 
            ON {self.collection_name} (json_data ->> '$.{json_key}')
        """
        self.cursor.execute(query)
        self.conn.commit()
        return self

    def insert(self, data_dict: dict) -> Optional[int]:
        try:
            self.cursor.execute(
                f"INSERT INTO {self.collection_name} (json_data) VALUES (?)",
                (json.dumps(data_dict),),
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.IntegrityError as e:
            print(f"Database Integrity Error: {e}")
            return None

    def select(self, key: str, value: Any, op: str = "=") -> List[Dict]:
        """Find matches using a specific operator (e.g., op=">" for age)."""
        if op.upper() not in self.ALLOWED_OPERATORS:
            raise ValueError(
                f"Operator {op} not allowed. Use one of: {self.ALLOWED_OPERATORS}"
            )

        query = f"""
            SELECT id, json_data, created_at 
            FROM {self.collection_name} 
            WHERE json_data ->> ? {op.upper()} ? 
            ORDER BY created_at DESC
        """
        # Add wildcard support if using LIKE
        if op.upper() == "LIKE" and isinstance(value, str) and "%" not in value:
            value = f"%{value}%"

        self.cursor.execute(query, (f"$.{key}", value))
        return self._format_results(self.cursor.fetchall())

    def update_field(self, record_id: int, key: str, new_value: Any):
        query = f"UPDATE {self.collection_name} SET json_data = json_set(json_data, ?, ?) WHERE id = ?"
        self.cursor.execute(query, (f"$.{key}", new_value, record_id))
        self.conn.commit()
        return self

    def update_row(self, record_id: int, data_dict: dict):
        query = f"UPDATE {self.collection_name} SET json_data = ? WHERE id = ?"
        self.cursor.execute(query, (json.dumps(data_dict), record_id))
        self.conn.commit()
        return self

    def delete(self, record_id: int) -> bool:
        self.cursor.execute(
            f"DELETE FROM {self.collection_name} WHERE id = ?", (record_id,)
        )
        self.conn.commit()
        return self.cursor.rowcount > 0

    def get_all(self) -> List[Dict]:
        self.cursor.execute(
            f"SELECT id, json_data, created_at FROM {self.collection_name} ORDER BY created_at DESC"
        )
        return self._format_results(self.cursor.fetchall())

    def _format_results(self, rows) -> List[Dict]:
        results = []
        for record_id, json_str, created_at in rows:
            results.append(
                {
                    "_meta": {"id": record_id, "created_at": created_at},
                    "data": json.loads(json_str),
                }
            )
        return results

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
