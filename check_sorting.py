from datetime import datetime
import os


def is_sorted_by_timestamp(file_path):
    """Checks if a TXT file is sorted by timestamp.

  Args:
    file_path (str): The path to the TXT file.

  Returns:
    bool: True if the file is sorted by timestamp, False otherwise.
  """

    with open(file_path, 'r') as file:
        lines = file.readlines()
        timestamps = [datetime.strptime(line.split(",")[1], "%Y-%m-%d %H:%M:%S") for line in lines]
        return all(a <= b for a, b in zip(timestamps, timestamps[1:]))


def check_all_txt_files(folder_path):
    """Checks if all TXT files in a folder are sorted by timestamp.

  Args:
    folder_path (str): The path to the folder containing TXT files.
  """

    for filename in os.listdir(folder_path):
        if filename.endswith(".txt"):
            file_path = os.path.join(folder_path, filename)
            if is_sorted_by_timestamp(file_path):
                print(f"/")
            else:
                print(f"{filename} is NOT sorted by timestamp...........................................")


if __name__ == "__main__":
    # Replace 'your_folder_path' with the actual path to your folder
    folder_path = 'D:/T-drive Taxi Trajectories/release/renamed'
    check_all_txt_files(folder_path)
