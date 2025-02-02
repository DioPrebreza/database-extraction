import mysql.connector
import csv
from tqdm import tqdm

# Database connection details
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "telegrafi",
    "connection_timeout": None
}

# SQL query for image fields
image_query = """
SELECT DISTINCT
    p.ID AS post_id,
    p.post_title AS post_headline,
    p.post_name AS post_slug,
    p.post_title AS post_title,
    GROUP_CONCAT(c.name ORDER BY c.name ASC) AS post_category,
    p.post_content AS content_body,
    p.post_status AS status,
    GROUP_CONCAT(t.name ORDER BY t.name ASC) AS tags,
    (SELECT meta_value FROM wp_postmeta WHERE post_id = p.ID AND meta_key = '_thumbnail_id' LIMIT 1) AS lead_media_image_id,
    (SELECT meta_value FROM wp_postmeta WHERE post_id = p.ID AND meta_key = '_wp_attachment_metadata' LIMIT 1) AS lead_media_image_data,
    (SELECT meta_value FROM wp_postmeta WHERE post_id = p.ID AND meta_key = '_edit_last' LIMIT 1) AS author_id,
    p.post_date AS publishing_date,
    p.post_modified AS modified_date,
    (SELECT meta_value FROM wp_postmeta WHERE post_id = p.ID AND meta_key = '_aioseo_title' LIMIT 1) AS meta_title,
    (SELECT meta_value FROM wp_postmeta WHERE post_id = p.ID AND meta_key = '_aioseo_description' LIMIT 1) AS meta_description
FROM 
    wp_posts p
LEFT JOIN 
    wp_term_relationships tr ON p.ID = tr.object_id
LEFT JOIN 
    wp_term_taxonomy tt ON tr.term_taxonomy_id = tt.term_taxonomy_id
LEFT JOIN 
    wp_terms c ON tt.term_id = c.term_id AND tt.taxonomy = 'category'  -- for post categories
LEFT JOIN
    wp_term_relationships tr_tag ON p.ID = tr_tag.object_id
LEFT JOIN
    wp_term_taxonomy tt_tag ON tr_tag.term_taxonomy_id = tt_tag.term_taxonomy_id  -- Corrected this line
LEFT JOIN
    wp_terms t ON tt_tag.term_id = t.term_id AND tt_tag.taxonomy = 'post_tag'  -- for post tags
WHERE 
    p.post_type = 'post'
    AND p.post_status = 'publish'  -- Only published posts
GROUP BY 
    p.ID
ORDER BY 
    p.post_date DESC
LIMIT 500;
"""

# Function to execute query and save results to CSV
def execute_query_to_csv(query, filename, headers):
    try:
        # Establish connection
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Execute query directly (no parameters)
        cursor.execute(query)
        results = cursor.fetchall()

        # Write results to CSV
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)  # Write header
            
            for row in tqdm(results, desc="Writing rows", unit="row"):
                writer.writerow(row)  # Write each data row

        print(f"Data has been written to {filename}")

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed")

# Define headers for your CSV (same order as in your SQL query)
headers = [
    "post_id", 
    "post_headline", 
    "post_slug", 
    "post_title",
    "post_category",
]

# Execute query and save to CSV file
execute_query_to_csv(image_query, "test1.csv", headers)
