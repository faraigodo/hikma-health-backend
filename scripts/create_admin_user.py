import psycopg
import bcrypt

DATABASE_URL = "postgresql://fst_hikma_db_user:wuuM5UuL4vDKjQye1ALsuzMavvuMNdcG@dpg-d0s9nlvdiees73a6hegg-a/fst_hikma_db"

def create_admin_user(username, email, password, role='admin'):
    salt = bcrypt.gensalt()
    hashed_password = bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

    sql = """
    INSERT INTO users (username, email, password_hash, role, created_at)
    VALUES (%s, %s, %s, %s, NOW())
    ON CONFLICT (username) DO NOTHING;
    """

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (username, email, hashed_password, role))
                print(f"Admin user '{username}' created or already exists.")
    except Exception as e:
        print(f"Error creating admin user: {e}")

if __name__ == "__main__":
    admin_username = "admin"
    admin_email = "godot@fst.co.zw"
    admin_password = "admin123"

    create_admin_user(admin_username, admin_email, admin_password)