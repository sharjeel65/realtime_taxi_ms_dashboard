import os
import pandas as pd
from datetime import datetime
import shutil

# Define the directory containing the taxi files
directory = 'D:/T-drive Taxi Trajectories/release/taxi_log_2008_by_id'
# Define the directory to save renamed files
renamed_directory = 'D:/T-drive Taxi Trajectories/release/renamed'

# Ensure the renamed directory exists
os.makedirs(renamed_directory, exist_ok=True)



# Function to extract the earliest timestamp from a file
def get_earliest_timestamp(file_path):
    try:
        df = pd.read_csv(file_path, header=None, names=['taxi_id', 'datetime', 'longitude', 'latitude'])
        # Specify the datetime format
        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        earliest_timestamp = df['datetime'].min()
        if pd.isna(earliest_timestamp):
            raise ValueError("Invalid timestamp")
        return earliest_timestamp
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None


# Create a list of files with their earliest timestamp
file_timestamps = []
for filename in os.listdir(directory):
    if filename.endswith(".txt"):  # Adjust the extension if necessary
        file_path = os.path.join(directory, filename)
        earliest_timestamp = get_earliest_timestamp(file_path)
        if earliest_timestamp is not None:
            file_timestamps.append((file_path, earliest_timestamp))

# Sort files by their earliest timestamp
file_timestamps.sort(key=lambda x: x[1])

# Rename files based on their earliest timestamp
for file_path, timestamp in file_timestamps:
    try:
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")
        new_filename = f"{timestamp_str}-{os.path.basename(file_path)}"
        new_file_path = os.path.join(renamed_directory, new_filename)

        # Use shutil.move to handle moving files across different drives
        shutil.move(file_path, new_file_path)
        print(f"Renamed {file_path} to {new_file_path}")
    except Exception as e:
        print(f"Error renaming file {file_path}: {e}")