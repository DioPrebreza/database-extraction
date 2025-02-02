import mysql.connector
import phpserialize

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="telegrafi"
)
cursor = conn.cursor()

# Get the serialized data
cursor.execute("SELECT option_value FROM wp_options WHERE option_name = 'wpseo-premium-redirects-base';")
row = cursor.fetchone()
if row:
    serialized_data = row[0]  # Extract the serialized string
    redirects = phpserialize.loads(serialized_data.encode())  # Decode PHP serialization

    # Print old → new URLs
    print("Old URL -> New URL")
    for redirect_id, data in redirects.items():
        print(f"{data[b'origin'].decode()} -> {data[b'url'].decode()}")

    # Optionally, save to a CSV file
    import csv
    with open("redirects.csv", "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Old URL", "New URL"])
        for data in redirects.values():
            writer.writerow([data[b"origin"].decode(), data[b"url"].decode()])

cursor.close()
conn.close()
