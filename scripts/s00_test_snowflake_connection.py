import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def main():
    print("🔍 Testing Snowflake connection...")

    try:
        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            authenticator="SNOWFLAKE_JWT",
            private_key_file=os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PATH"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )

        cur = conn.cursor()
        cur.execute("""
            SELECT
                CURRENT_USER(),
                CURRENT_ROLE(),
                CURRENT_WAREHOUSE(),
                CURRENT_DATABASE(),
                CURRENT_SCHEMA(),
                CURRENT_VERSION()
        """)
        row = cur.fetchone()

        print("✅ Connected successfully")
        print(f"User      : {row[0]}")
        print(f"Role      : {row[1]}")
        print(f"Warehouse : {row[2]}")
        print(f"Database  : {row[3]}")
        print(f"Schema    : {row[4]}")
        print(f"Version   : {row[5]}")

    except Exception as e:
        print("❌ Snowflake connection failed")
        raise e
    finally:
        try:
            conn.close()
            print("🔌 Connection closed")
        except Exception:
            pass


if __name__ == "__main__":
    main()
