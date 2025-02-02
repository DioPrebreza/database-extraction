import csv

def count_rows_in_csv(filename):
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            row_count = sum(1 for row in reader)  # Count the rows
        return row_count
    except Exception as e:
        print(f"Error: {e}")

# Example usage
filename = 'images.csv'
rows = count_rows_in_csv(filename)
print(f"The CSV file has {rows} rows.")
