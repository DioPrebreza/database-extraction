import mysql.connector

# Establishing the connection to the database
conn = mysql.connector.connect(
    host="localhost",      # Replace with your database host
    user="root",  # Replace with your MySQL username
    password="root",  # Replace with your MySQL password
    database="telegrafi"  # Replace with your database name
)

# Creating a cursor object to interact with the database
cursor = conn.cursor()

# SQL query to count rows in wp_posts
query = "SELECT COUNT(*) FROM wp_posts;"

# Executing the query
cursor.execute(query)

# Fetching the result
result = cursor.fetchone()

# Printing the result
print(f"Total posts: {result[0]}")

# Closing the cursor and connection
cursor.close()
conn.close()
