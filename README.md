# flexdb

noSQL-like dev experence on a relational engine.

## Features

* **NoSQL API:** Insert dictionaries, update specific JSON fields, and query by JSON keys.
* **Collections:** Instantiating multiple classes against the same `.db` file automatically isolates data into separate tables (collections).
* **Advanced Querying:** Supports exact matches, inequalities (`>`, `<`), and fuzzy matching (`LIKE`) directly on JSON fields.
* **Safe & Secure:** Fully parameterized queries prevent SQL injection, even in JSON paths and table names.
* **Extensible:** Implements a `DocumentDB` Abstract Base Class, allowing seamless swapping to other databases (e.g., PostgreSQL `JSONB`) as your app scales.

## Requirements

* Python 3.7+ (SQLite 3.38+ recommended for optimal JSON path support)
* No external dependencies (uses standard library `sqlite3`, `json`, `re`).

## Usage

### 1. Initialization & Collections

Create independent collections sharing the same database file.

```python
from flexdb import SQLiteFlexDB

# Initialize collections
users = SQLiteFlexDB("app.db", collection_name="users")
logs = SQLiteFlexDB("app.db", collection_name="logs")

```

### 2. Inserting Data

Pass standard Python dictionaries. The ID is automatically generated.

```python
user_id = users.insert({
    "name": "Alice",
    "email": "alice@example.com",
    "age": 28,
    "role": "admin"
})
print(f"Inserted user {user_id}")

```

### 3. Indexing (Primary Key behavior for JSON fields)

To enforce uniqueness or speed up queries on a specific JSON key, create an index.

```python
# Create a unique index on the email field
users.create_index(json_key="email", unique=True)

# Create a standard index for fast lookups on age
users.create_index(json_key="age", unique=False)

```

### 4. Querying Data

Results are returned as a list of dictionaries. To prevent metadata collisions with your JSON payload, results are strictly partitioned into `_meta` (database ID and timestamps) and `data` (your JSON document).

```python
# Exact match
admin_users = users.select("role", "admin")

# Greater than / Less than
adults = users.select("age", 18, op=">=")

# Fuzzy search (LIKE)
gmail_users = users.select("email", "@gmail.com", op="LIKE")

print(adults[0])
# Output format:
# {
#     "_meta": {"id": 1, "created_at": "2023-10-21 14:00:00"},
#     "data": {"name": "Alice", "email": "alice@example.com", "age": 28, "role": "admin"}
# }

```

### 5. Updating Data

You can replace the entire document or update a single specific field inside the JSON blob.

```python
# Update a specific field (leaves the rest of the JSON intact)
users.update_field(record_id=1, key="age", new_value=29)

# Replace the entire JSON document
users.update_row(record_id=1, data_dict={
    "name": "Alice",
    "email": "alice_new@example.com",
    "age": 29,
    "role": "superadmin"
})

```

### 6. Deleting Data

```python
success = users.delete(record_id=1)
if success:
    print("Record deleted successfully.")

```

### 7. Fetching All Records

```python
all_users = users.get_all()

```

### Context Manager Support

You can safely use the database instance as a context manager to ensure connections are closed automatically.

```python
with SQLiteFlexDB("app.db", "analytics") as events:
    events.insert({"event": "click", "element": "buy_button"})

```