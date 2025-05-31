import psycopg

# Replace this with your Internal Database URL exactly as shown in Render dashboard
DATABASE_URL = "postgresql://fst_hikma_db_user:wuuM5UuL4vDKjQye1ALsuzMavvuMNdcG@dpg-d0s9nlvdiees73a6hegg-a/fst_hikma_db"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tokens (
    token VARCHAR(255) PRIMARY KEY,
    user_id INTEGER NOT NULL,
    expiry TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
"""

def create_tokens_table():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)
                print("Tokens table created or already exists.")
    except Exception as e:
        print(f"Error creating tokens table: {e}")

if __name__ == "__main__":
    create_tokens_table()
