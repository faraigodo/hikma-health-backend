import psycopg

DB_CONN_INFO = "postgresql://fst_hikma_db_user:wuuM5UuL4vDKjQye1ALsuzMavvuMNdcG@dpg-d0s9nlvdiees73a6he.postgres.render.com:5432/fst_hikma_db"

CREATE_TOKENS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tokens (
    token VARCHAR(255) PRIMARY KEY,
    user_id INTEGER NOT NULL,
    expiry TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
"""

def ensure_tokens_table_exists():
    try:
        with psycopg.connect(DB_CONN_INFO) as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_TOKENS_TABLE_SQL)
                print("Checked and ensured 'tokens' table exists.")
    except Exception as e:
        print(f"Error ensuring tokens table: {e}")

if __name__ == "__main__":
    ensure_tokens_table_exists()
