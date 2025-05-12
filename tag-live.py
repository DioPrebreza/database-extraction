import mysql.connector
import csv
import os

# Database connection details
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "telegrafi",
    "connection_timeout": 300
}

# SQL query to get livefeed tags with shortcode_id
query = """
SELECT 
  t.term_id, 
  t.name, 
  t.slug,
  CONCAT('[live_feed id="', t.term_id, '"]') AS shortcode
FROM wp_terms t
JOIN wp_term_taxonomy tt ON t.term_id = tt.term_id
WHERE tt.taxonomy = 'livefeed_tag'
"""

def fetch_livefeed_tags():
    """Fetch livefeed tags with shortcode_id"""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute(query)
        rows = cursor.fetchall()

        if rows:
            return rows
        else:
            print("No data found for livefeed tags.")
            return []
    
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        return []
    
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("Database connection closed.")

def export_to_csv(rows):
    """Export rows to CSV"""
    if rows:
        headers = list(rows[0].keys())  # Get column names from the first row
        output_filename = 'livefeed_tags.csv'

        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)

        print(f"Exported {len(rows)} rows to {output_filename}")
    else:
        print("No data to export.")

if __name__ == "__main__":
    # Fetch livefeed tags
    livefeed_tags = fetch_livefeed_tags()

    # Export the fetched tags to a CSV file
    export_to_csv(livefeed_tags)
