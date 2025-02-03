import sqlite3

def find_shortest_path(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
WITH RECURSIVE PathCTE AS (
    SELECT
        from_id,
        to_id,
        to_table,
        weight,
        from_table || '.' || from_column || ' -> ' || to_table || '.' || to_column AS path,
        1 AS depth
    FROM Edges
    WHERE from_table = 'Invoice'

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
    WHERE p.to_table != 'Album'
      AND NOT p.path LIKE '%' || e.to_table || '.' || e.to_column || '%'
)
SELECT
    path,
    depth
FROM PathCTE
WHERE to_table = 'Album'
ORDER BY weight
LIMIT 1;
""")
#AND NOT p.path LIKE '%' || e.to_id || '%'
    response = cursor.fetchall()
    return str(response[0][0])

print(find_shortest_path('Chinook_Sqlite.sqlite'))
