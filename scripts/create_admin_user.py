import psycopg
import bcrypt
import os
import uuid

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://fst_hikma_db_user:wuuM5UuL4vDKjQye1ALsuzMavvuMNdcG@dpg-d0s9nlvdiees73a6hegg-a/fst_hikma_db")

def create_admin_user(name, email, password, role='admin'):
    user_id = str(uuid.uuid4())
    salt = bcrypt.gensalt()
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

    sql = """
    INSERT INTO users (id, name, email, hashed_password, role, is_deleted)
    VALUES (%s, %s, %s, %s, %s, FALSE)
    ON CONFLICT (email) DO NOTHING;
    """

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (user_id, name, email, hashed_password, role))
                print(f"Admin user '{name}' created or already exists.")
    except Exception as e:
        print(f"Error creating admin user: {e}")

if __name__ == "__main__":
    admin_name = "admin"
    admin_email = "godot@fst.co.zw"
    admin_password = "admin1234"

    create_admin_user(admin_name, admin_email, admin_password)