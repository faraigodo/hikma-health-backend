import psycopg

# Use your actual Internal Database URL here
DATABASE_URL = "postgresql://fst_hikma_db_user:wuuM5UuL4vDKjQye1ALsuzMavvuMNdcG@dpg-d0s9nlvdiees73a6hegg-a/fst_hikma_db"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS server_variables (
    id SERIAL PRIMARY KEY,
    key VARCHAR(255) UNIQUE NOT NULL,
    value TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
"""

def create_server_variables_table():
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_TABLE_SQL)
                print("server_variables table created or already exists.")
    except Exception as e:
        print(f"Error creating server_variables table: {e}")

if __name__ == "__main__":
    create_server_variables_table()
