import sqlite3
import json

class FlexDB:
    def __init__(self, db_name, table_name="flex"):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.table_name = table_name
        self.create_table()

    def create_table(self):
        """Create a JSON table"""

        query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                json_data TEXT CHECK(json_valid(json_data)),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        self.cursor.execute(query)
        self.conn.commit()

        return self

    def insert(self, data_dict: dict):
        """Insert dict in a JSON table"""

        json_str = json.dumps(data_dict)
        try:
            self.cursor.execute(
                f"INSERT INTO {self.table_name} (json_data) VALUES (?)", (json_str,)
            )
            self.conn.commit()
            print(f"Saved record ID: {self.cursor.lastrowid}")
        except sqlite3.IntegrityError:
            print("Error: Database rejected invalid JSON data.")

        return self

    def select(self, key, value):
        """Find key/value pair in the rows of a JSON table"""

        query = f"SELECT id, json_data, created_at FROM {self.table_name} WHERE json_data ->> ? = ? ORDER BY created_at DESC"
        self.cursor.execute(query, (key, value))

        results = []
        for row in self.cursor.fetchall():
            record_id, json_str, created_at = row
            data = json.loads(json_str)
            data["db_id"] = row[0]
            data["id"] = record_id
            data["created_at"] = created_at
            results.append(data)
        return results

    def update_field(self, record_id, key, new_value):
        """Update a field in the JSON table"""

        # json_set updates an existing key or adds it if missing
        query = f"UPDATE {self.table_name} SET json_data = json_set(json_data, '$.{key}', ?) WHERE id = ?"
        self.cursor.execute(query, (new_value, record_id))
        self.conn.commit()

        print(f"Updated field '{key}' for ID {record_id}")

        return self


    def delete(self, record_id):
        self.cursor.execute(f"DELETE FROM {self.table_name} WHERE id = ?", (record_id,))
        self.conn.commit()
        return self.cursor.rowcount > 0

    def close(self):
        self.conn.close()

    # Use as context mananger
    def __enter__(self):
        return self

    def __exit__(self, ctx_type, ctx_value, ctx_traceback):
        self.close()
