import mysql.connector
import csv
from datetime import datetime

# Database connection details
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "telegrafi",
    "connection_timeout": 300
}

# Query to extract both caption and credit from meta_value
test_query = """
SELECT 
    p.ID as post_id,
    p.post_title as post_title,
    att.ID as attachment_id,
    att.guid as image_url,
    SUBSTRING_INDEX(
        SUBSTRING_INDEX(
            pm.meta_value,
            '"caption";s:',
            -1
        ),
        '";',
        1
    ) as image_caption,
    SUBSTRING_INDEX(
        SUBSTRING_INDEX(
            pm.meta_value,
            '"credit";s:',
            -1
        ),
        '";',
        1
    ) as image_credit
FROM 
    wp_posts p
JOIN 
    wp_postmeta thumbnail ON p.ID = thumbnail.post_id 
    AND thumbnail.meta_key = '_thumbnail_id'
JOIN 
    wp_posts att ON att.ID = thumbnail.meta_value
JOIN 
    wp_postmeta pm ON att.ID = pm.post_id
WHERE 
    p.post_type = 'post'
    AND p.post_status = 'publish'
    AND pm.meta_key = '_wp_attachment_metadata'
    AND (
        pm.meta_value LIKE '%caption%'
        OR pm.meta_value LIKE '%credit%'
    )
    AND pm.meta_value NOT LIKE '%s:0:""%'
ORDER BY 
    p.ID DESC
LIMIT 10;
"""

def test_image_query():
    """Test the query and output results to CSV"""
    try:
        # Connect to database
        print("Connecting to database...")
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        # Execute query
        print("Executing query...")
        cursor.execute(test_query)
        rows = cursor.fetchall()
        
        if rows:
            # Prepare CSV file
            filename = f'image_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            headers = list(rows[0].keys())
            
            # Write to CSV
            print(f"Writing results to {filename}...")
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)
            
            print(f"Successfully wrote {len(rows)} rows to {filename}")
            
            # Print first row for verification
            print("\nFirst row of results:")
            for key, value in rows[0].items():
                print(f"{key}: {value}")
                
            return True
        else:
            print("No results found")
            return False
    
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        return False
    
    except Exception as e:
        print(f"Error: {e}")
        return False
    
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("Database connection closed")

if __name__ == "__main__":
    test_image_query()