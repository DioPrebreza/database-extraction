import mysql.connector
import csv
import os
from datetime import datetime
from tqdm import tqdm

# Database connection details
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "telegrafi",
    "connection_timeout": 300,
}

# SQL query for testing with 100 rows
test_query = """
SELECT 
    p.ID AS post_id,
    p.post_title AS post_headline,

    CASE 
        WHEN p.post_type = 'sponsored' THEN (
            SELECT p2.post_content
            FROM wp_posts p2
            WHERE p2.ID = (
                SELECT meta_value 
                FROM wp_postmeta 
                WHERE post_id = p.ID AND meta_key = 'article'
                LIMIT 1
            )
        )
        ELSE p.post_content
    END AS content_body,

    p.post_name AS post_slug,
    p.post_title AS post_title,
    p.post_type AS post_type,

    GROUP_CONCAT(DISTINCT t.name ORDER BY t.name ASC SEPARATOR ', ') AS category,

    COALESCE(
        (SELECT wp.guid 
         FROM wp_posts wp 
         INNER JOIN wp_postmeta wpm ON wp.ID = wpm.meta_value 
         WHERE wpm.post_id = p.ID AND wpm.meta_key = '_thumbnail_id' 
         LIMIT 1),
        ''
    ) AS image_url,

    CASE 
        WHEN SUBSTRING_INDEX(
            SUBSTRING_INDEX(
                (SELECT pm.meta_value 
                 FROM wp_postmeta pm 
                 INNER JOIN wp_postmeta wpm ON pm.post_id = wpm.meta_value 
                 WHERE wpm.post_id = p.ID 
                 AND wpm.meta_key = '_thumbnail_id' 
                 AND pm.meta_key = '_wp_attachment_metadata'
                 LIMIT 1),
                '"caption";s:', -1
            ),
            '";', 1
        ) LIKE '0:"' THEN ''
        ELSE COALESCE(
            SUBSTRING_INDEX(
                SUBSTRING_INDEX(
                    (SELECT pm.meta_value 
                     FROM wp_postmeta pm 
                     INNER JOIN wp_postmeta wpm ON pm.post_id = wpm.meta_value 
                     WHERE wpm.post_id = p.ID 
                     AND wpm.meta_key = '_thumbnail_id' 
                     AND pm.meta_key = '_wp_attachment_metadata'
                     LIMIT 1),
                    '"caption";s:', -1
                ),
                '";', 1
            ),
            ''
        )
    END AS image_caption,

    CASE 
        WHEN SUBSTRING_INDEX(
            SUBSTRING_INDEX(
                (SELECT pm.meta_value 
                 FROM wp_postmeta pm 
                 INNER JOIN wp_postmeta wpm ON pm.post_id = wpm.meta_value 
                 WHERE wpm.post_id = p.ID 
                 AND wpm.meta_key = '_thumbnail_id' 
                 AND pm.meta_key = '_wp_attachment_metadata'
                 LIMIT 1),
                '"credit";s:', -1
            ),
            '";', 1
        ) LIKE '0:"' THEN ''
        ELSE COALESCE(
            SUBSTRING_INDEX(
                SUBSTRING_INDEX(
                    (SELECT pm.meta_value 
                     FROM wp_postmeta pm 
                     INNER JOIN wp_postmeta wpm ON pm.post_id = wpm.meta_value 
                     WHERE wpm.post_id = p.ID 
                     AND wpm.meta_key = '_thumbnail_id' 
                     AND pm.meta_key = '_wp_attachment_metadata'
                     LIMIT 1),
                    '"credit";s:', -1
                ),
                '";', 1
            ),
            ''
        )
    END AS image_credit,

    (SELECT meta_value 
     FROM wp_postmeta 
     WHERE post_id = p.ID AND meta_key = 'video_embed' 
     LIMIT 1) AS raw_video_embed,

    (SELECT meta_value 
     FROM wp_postmeta 
     WHERE post_id = p.ID AND meta_key = 'article' 
     LIMIT 1) AS linked_article_id,

    p.post_excerpt AS excerpt,

    CONCAT(u.display_name) AS author,
    p.post_date AS publishing_date,
    p.post_modified AS modified_date

FROM 
    wp_posts p
LEFT JOIN wp_term_relationships tr ON p.ID = tr.object_id
LEFT JOIN wp_term_taxonomy tt ON tr.term_taxonomy_id = tt.term_taxonomy_id
LEFT JOIN wp_terms t ON tt.term_id = t.term_id
LEFT JOIN wp_users u ON p.post_author = u.ID

WHERE 
    p.post_type IN ('video', 'sponsored', 'live-video')
    AND p.post_status = 'publish'
GROUP BY p.ID
ORDER BY p.ID DESC
LIMIT 10;
"""

# Modified query for batch processing
batch_query = batch_query = """
SELECT 
    p.ID AS post_id,
    p.post_title AS post_headline,

    CASE 
        WHEN p.post_type = 'sponsored' THEN (
            SELECT p2.post_content
            FROM wp_posts p2
            WHERE p2.ID = (
                SELECT meta_value 
                FROM wp_postmeta 
                WHERE post_id = p.ID AND meta_key = 'article'
                LIMIT 1
            )
        )
        ELSE p.post_content
    END AS content_body,

    p.post_name AS post_slug,
    p.post_title AS post_title,
    p.post_type AS post_type,

    GROUP_CONCAT(DISTINCT t.name ORDER BY t.name ASC SEPARATOR ', ') AS category,

    COALESCE(
        (SELECT wp.guid 
         FROM wp_posts wp 
         INNER JOIN wp_postmeta wpm ON wp.ID = wpm.meta_value 
         WHERE wpm.post_id = p.ID AND wpm.meta_key = '_thumbnail_id' 
         LIMIT 1),
        ''
    ) AS image_url,

    CASE 
        WHEN SUBSTRING_INDEX(
            SUBSTRING_INDEX(
                (SELECT pm.meta_value 
                 FROM wp_postmeta pm 
                 INNER JOIN wp_postmeta wpm ON pm.post_id = wpm.meta_value 
                 WHERE wpm.post_id = p.ID 
                 AND wpm.meta_key = '_thumbnail_id' 
                 AND pm.meta_key = '_wp_attachment_metadata'
                 LIMIT 1),
                '"caption";s:', -1
            ),
            '";', 1
        ) LIKE '0:"' THEN ''
        ELSE COALESCE(
            SUBSTRING_INDEX(
                SUBSTRING_INDEX(
                    (SELECT pm.meta_value 
                     FROM wp_postmeta pm 
                     INNER JOIN wp_postmeta wpm ON pm.post_id = wpm.meta_value 
                     WHERE wpm.post_id = p.ID 
                     AND wpm.meta_key = '_thumbnail_id' 
                     AND pm.meta_key = '_wp_attachment_metadata'
                     LIMIT 1),
                    '"caption";s:', -1
                ),
                '";', 1
            ),
            ''
        )
    END AS image_caption,

    CASE 
        WHEN SUBSTRING_INDEX(
            SUBSTRING_INDEX(
                (SELECT pm.meta_value 
                 FROM wp_postmeta pm 
                 INNER JOIN wp_postmeta wpm ON pm.post_id = wpm.meta_value 
                 WHERE wpm.post_id = p.ID 
                 AND wpm.meta_key = '_thumbnail_id' 
                 AND pm.meta_key = '_wp_attachment_metadata'
                 LIMIT 1),
                '"credit";s:', -1
            ),
            '";', 1
        ) LIKE '0:"' THEN ''
        ELSE COALESCE(
            SUBSTRING_INDEX(
                SUBSTRING_INDEX(
                    (SELECT pm.meta_value 
                     FROM wp_postmeta pm 
                     INNER JOIN wp_postmeta wpm ON pm.post_id = wpm.meta_value 
                     WHERE wpm.post_id = p.ID 
                     AND wpm.meta_key = '_thumbnail_id' 
                     AND pm.meta_key = '_wp_attachment_metadata'
                     LIMIT 1),
                    '"credit";s:', -1
                ),
                '";', 1
            ),
            ''
        )
    END AS image_credit,

    (SELECT meta_value 
     FROM wp_postmeta 
     WHERE post_id = p.ID AND meta_key = 'video_embed' 
     LIMIT 1) AS raw_video_embed,

    (SELECT meta_value 
     FROM wp_postmeta 
     WHERE post_id = p.ID AND meta_key = 'article' 
     LIMIT 1) AS linked_article_id,

    p.post_excerpt AS excerpt,

    CONCAT(u.display_name) AS author,
    p.post_date AS publishing_date,
    p.post_modified AS modified_date

FROM 
    wp_posts p
LEFT JOIN wp_term_relationships tr ON p.ID = tr.object_id
LEFT JOIN wp_term_taxonomy tt ON tr.term_taxonomy_id = tt.term_taxonomy_id
LEFT JOIN wp_terms t ON tt.term_id = t.term_id
LEFT JOIN wp_users u ON p.post_author = u.ID

WHERE 
    p.post_type IN ('video', 'sponsored', 'live-video')
    AND p.post_status = 'publish'
    AND p.ID > %s

GROUP BY p.ID
ORDER BY p.ID
LIMIT %s;
"""


def test_single_row():
    """Test the query with 100 rows"""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute(test_query)
        rows = cursor.fetchall()
        
        if rows:
            headers = list(rows[0].keys())
            
            # Write to CSV
            with open('posts_test.csv', 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)
            
            print(f"Test completed: {len(rows)} rows have been written to posts_test.csv")
            return True
        
        return False
    
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return False
    
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

def process_in_batches(batch_size=10000):
    """Process all posts in batches"""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        
        # Get total count first
        print("Counting total posts...")
        cursor.execute("SELECT COUNT(*) as total FROM wp_posts WHERE post_type IN ('video', 'sponsored', 'live-video') AND post_status = 'publish'")
        total_posts = cursor.fetchone()['total']
        print(f"Total posts to process: {total_posts:,}")
        
        last_id = -1
        file_counter = 1
        total_processed = 0
        
        while True:
            cursor.execute(batch_query, (last_id, batch_size))
            rows = cursor.fetchall()
            
            if not rows:
                break
                
            # Write batch to CSV
            filename = f'posts_VSL_{file_counter}.csv'
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
            
            batch_count = len(rows)
            total_processed += batch_count
            
            print(f"\nBatch {file_counter}:")
            print(f"- Wrote {batch_count:,} rows to {filename}")
            print(f"- Total processed: {total_processed:,} of {total_posts:,} posts")
            print(f"- Progress: {(total_processed/total_posts)*100:.2f}%")
            print(f"- Last ID processed: {rows[-1]['post_id']}")
            
            last_id = rows[-1]['post_id']
            file_counter += 1
        
        print("\nProcessing complete!")
        print(f"Total files created: {file_counter-1}")
        print(f"Total rows processed: {total_processed:,}")
        
        if total_processed != total_posts:
            print(f"\nWARNING: Processed posts ({total_processed:,}) doesn't match total posts ({total_posts:,})")
            print("This might be due to posts being added or removed during processing")
        else:
            print("\nSuccess! All posts processed correctly")
            
    except mysql.connector.Error as err:
        print(f"Database Error: {err}")
    
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("\nDatabase connection closed")

if __name__ == "__main__":
    # First test with a single row
    print("Testing with a single row...")
    if test_single_row():
        input("Check posts_test.csv and press Enter to continue with batch processing...")
        process_in_batches()
    else:
        print("Test failed. Please check your database connection and query.")