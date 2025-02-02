import mysql.connector

# Database connection details
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "telegrafi",
    "connection_timeout": None
}

# SQL query to count the number of records
image_query = """
SELECT COUNT(DISTINCT wp_posts.ID) AS total_posts
FROM wp_posts
WHERE wp_posts.post_type = 'post'
AND wp_posts.post_status = 'publish';
"""

# Function to execute query and get row count
def execute_query_for_row_count(query):
    try:
        # Establish connection
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Execute the count query
        cursor.execute(query)
        result = cursor.fetchone()  # Fetch the count from the query result

        # Get the count value from the result
        row_count = result[0]
        print(f"Total number of rows: {row_count}")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed")

# Execute query and get row count
execute_query_for_row_count(image_query)
