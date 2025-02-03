import os
import sqlite3

def generate_graph_sql(db_path):
    """从数据库 Schema 生成节点和边表的 SQL 插入语句。"""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
WITH table_info AS (
    SELECT
        m.name AS table_name,
        p.*
    FROM sqlite_master AS m
    JOIN pragma_table_info(m.name) AS p
    WHERE m.type = 'table'
),
foreign_key_info AS (
    SELECT
        m.name AS table_name,
        fk.*
    FROM sqlite_master AS m
    JOIN pragma_foreign_key_list(m.name) AS fk
    WHERE m.type = 'table'
)
SELECT
    ti.table_name,
    fki."table" AS foreign_key_table,
    fki."from" AS foreign_key_column,
    fki."to" AS foreign_key_referenced_column
FROM table_info AS ti
LEFT JOIN foreign_key_info AS fki
    ON ti.table_name = fki.table_name
    AND ti.name = fki."from"
ORDER BY ti.table_name, ti.cid;
    """)

    schema_info = cursor.fetchall()

    nodes = set()
    edges = []

    for row in schema_info:
        table_name, foreign_key_table, foreign_key_column, foreign_key_referenced_column = row

        # 添加节点
        nodes.add(table_name)

        # 添加边
        if foreign_key_table:
            edges.append((table_name, foreign_key_table, foreign_key_column, foreign_key_referenced_column))
            edges.append((foreign_key_table, table_name, foreign_key_referenced_column, foreign_key_column))
    node_inserts = []
    for i, node in enumerate(nodes):
        node_inserts.append(f"INSERT INTO Nodes (id, name) VALUES ({i+1}, '{node}');")

    edge_inserts = []
    for i, edge in enumerate(edges):
      from_node, to_node, from_col, to_col = edge
      from_node_id = list(nodes).index(from_node) +1
      to_node_id = list(nodes).index(to_node) + 1
      edge_inserts.append(f"INSERT INTO Edges (from_id, to_id, from_table, from_column, to_table, to_column, weight) VALUES ({from_node_id}, {to_node_id}, '{from_node}', '{from_col}', '{to_node}', '{to_col}', 1);")

    conn.close()

    return "\n".join(node_inserts) + "\n" + "\n".join(edge_inserts)


# 示例用法
db_file = 'Chinook_Sqlite.sqlite' # 替换成你的数据库文件路径
if not os.path.exists(db_file):
    raise Exception(f"Database file not found at {db_file}")
sql_inserts = generate_graph_sql(db_file)
print(sql_inserts)

# 如果要执行这些SQL语句，可以这样做：
conn = sqlite3.connect(db_file)
cursor = conn.cursor()
cursor.execute("CREATE TABLE Nodes (id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
cursor.execute("CREATE TABLE Edges (from_id INTEGER, to_id INTEGER, from_table TEXT, from_column TEXT, to_table TEXT, to_column TEXT, weight REAL DEFAULT 1, FOREIGN KEY (from_id) REFERENCES Nodes(id), FOREIGN KEY (to_id) REFERENCES Nodes(id));")
cursor.executescript(sql_inserts)
conn.commit()
conn.close()