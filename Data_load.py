import pandas as pd
import os

# Define the directory containing the text files
directory = 'C:/Users/mouni/Downloads/T-drive Taxi Trajectories/release/taxi_log_2008_by_id'

# Initialize an empty list to store the file contents
data = []

# Define the chunk size (number of lines to read at a time)
chunk_size = 1000  # Adjust this based on your memory constraints

# Loop through all files in the directory
for filename in os.listdir(directory):
    if filename.endswith('.txt'):
        file_path = os.path.join(directory, filename)
        with open(file_path, 'r', encoding='utf-8') as file:
            while True:
                chunk = file.readlines(chunk_size)
                if not chunk:
                    break
                data.append(''.join(chunk))

# Create a DataFrame with the collected data
df = pd.DataFrame(data, columns=['Content'])

# Save the DataFrame to a single file, such as CSV
output_file = 'D:/Mouni Folder/TAXI_DATA'
df.to_csv(output_file, index=False)