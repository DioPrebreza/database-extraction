import mysql.connector

# Database connection details
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "telegrafi",
    "connection_timeout": None
}
# Connect to the database
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # Step 1: List all tables
    cursor.execute("SHOW TABLES;")
    tables = [table[0] for table in cursor.fetchall()]
    print("\n🔍 Found Tables:\n", tables)

    # Step 2: Search for Yoast-related or Redirect-related tables
    redirect_tables = [table for table in tables if "redirect" in table.lower() or "yoast" in table.lower()]
    print("\n📌 Possible Redirect Tables:\n", redirect_tables)

    # Step 3: Check column names in these tables
    for table in redirect_tables:
        print(f"\n🔎 Checking columns in `{table}`:")
        cursor.execute(f"DESCRIBE {table};")
        columns = [column[0] for column in cursor.fetchall()]
        print("Columns:", columns)

        # Step 4: If a table has redirect-like columns, check how many rows exist
        if any(col in columns for col in ["old_url", "new_url", "redirect_from", "redirect_to", "status_code"]):
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            count = cursor.fetchone()[0]
            print(f"✅ Table `{table}` contains {count} redirects!")

            # Fetch a sample of 5 rows
            cursor.execute(f"SELECT * FROM {table} LIMIT 5;")
            sample_data = cursor.fetchall()
            print("\n🛠 Sample Data:\n", sample_data)

except mysql.connector.Error as err:
    print("❌ Database Error:", err)
finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()