import os
import csv

# Function to combine CSV files
def combine_csv(files, output_file):
    try:
        # Open the output file in write mode
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            
            # Iterate through each CSV file
            for idx, file in enumerate(files):
                with open(file, 'r', encoding='utf-8') as infile:
                    reader = csv.reader(infile)
                    # Write the header only for the first file
                    if idx == 0:
                        header = next(reader)  # Read and write the header
                        writer.writerow(header)
                    else:
                        next(reader)  # Skip the header of subsequent files
                    # Write the rest of the data rows
                    for row in reader:
                        writer.writerow(row)
        
        print(f"CSV files have been combined into {output_file}")

    except Exception as e:
        print(f"Error: {e}")

# List of CSV files to combine
csv_files = [
    "posts_1.csv",
    "posts_2.csv",
    "posts_3.csv",
]

# Output file where the combined data will be saved
output_csv = "posts.csv"

# Call the function to combine the files
combine_csv(csv_files, output_csv)
