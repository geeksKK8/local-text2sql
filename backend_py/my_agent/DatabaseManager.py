import requests
import sqlite3
import os
from typing import List, Any


class DatabaseManager:
    def __init__(self):
        self.endpoint_url = os.getenv("DB_ENDPOINT_URL")
        self.filePath = 'Chinook_Sqlite.sqlite'

    def get_schema(self) -> str:
        """Retrieve the database schema."""
        try:
            # response = requests.get(
            #     f"{self.endpoint_url}/get-schema/{uuid}"
            # )
            # response.raise_for_status()
            # return response.json()['schema']
            # filePath = 'Chinook_Sqlite.sqlite'
            if not os.path.exists(self.filePath):
                raise Exception(f"Database file not found at {self.filePath}")
            conn = sqlite3.connect(self.filePath)
            cursor = conn.cursor()
            cursor.execute("SELECT name, sql FROM sqlite_schema WHERE type='table';")
            schema = cursor.fetchall()
            conn.close()
            return str(schema)
        except requests.RequestException as e:
            raise Exception(f"Error fetching schema: {str(e)}")

    def execute_query(self, query: str) -> List[Any]:
        """Execute SQL query on the remote database and return results."""
        try:
            # response = requests.post(
            #     f"{self.endpoint_url}/execute-query",
            #     json={"uuid": uuid, "query": query}
            # )
            # response.raise_for_status()
            # return response.json()['results']
            # filePath = 'Chinook_Sqlite.sqlite'
            if not os.path.exists(self.filePath):
                raise Exception(f"Database file not found at {self.filePath}")
            conn = sqlite3.connect(self.filePath)
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            conn.close()
            return results
        except requests.RequestException as e:
            raise Exception(f"Error executing query: {str(e)}")
        
    def get_join_path(self, table1: str, table2: str) -> str:
        """Retrieve the join path between two tables."""
        conn = sqlite3.connect(self.filePath)
        cursor = conn.cursor() 
        cursor.execute(f"""
WITH RECURSIVE PathCTE AS (
    SELECT
        from_id,
        to_id,
        to_table,
        weight,
        from_table || '.' || from_column || ' -> ' || to_table || '.' || to_column AS path,
        1 AS depth
    FROM Edges
    WHERE from_table = '{table1}'

    UNION ALL

    SELECT
        e.from_id,
        e.to_id,
        e.to_table,
        p.weight + e.weight AS weight,
        p.path || ' -> ' || e.to_table || '.' || e.to_column AS path,
        p.depth + 1 AS depth
    FROM Edges e
    JOIN PathCTE p ON e.from_id = p.to_id
    WHERE p.to_table != '{table2}'
      AND NOT p.path LIKE '%' || e.to_table || '.' || e.to_column || '%'
)
SELECT
    path,
    depth
FROM PathCTE
WHERE to_table = '{table2}'
ORDER BY weight
LIMIT 1;
""")
        response = cursor.fetchall()
        return response
        
    