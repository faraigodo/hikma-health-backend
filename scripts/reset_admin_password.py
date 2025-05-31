import psycopg
import bcrypt
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://fst_hikma_db_user:wuuM5UuL4vDKjQye1ALsuzMavvuMNdcG@dpg-d0s9nlvdiees73a6hegg-a/fst_hikma_db")

def reset_admin_password(email, new_password):
    hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    sql = """
    UPDATE users
    SET hashed_password = %s
    WHERE email = %s;
    """

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (hashed_password, email))
                if cur.rowcount == 0:
                    print(f"No user found with email '{email}'")
                else:
                    print(f"Password updated successfully for '{email}'")
    except Exception as e:
        print(f"Error resetting password: {e}")

if __name__ == "__main__":
    admin_email = "godot@fst.co.zw"       # change to your admin email
    new_password = "newpassword123" # change to your desired new password

    reset_admin_password(admin_email, new_password)
