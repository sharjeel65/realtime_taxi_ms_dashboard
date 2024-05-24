import os


def read_txt_files(folder_path):
    """Reads all TXT files in a folder and prints their content to the console.

  Args:
    folder_path (str): The path to the folder containing the TXT files.
  """

    for filename in os.listdir(folder_path):
        if filename.endswith(".txt"):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as file:
                content = file.read()
                print(f"Contents of {filename}:")
                print(content)
                print("-" * 20)  # Optional separator between files


if __name__ == "__main__":
    # Replace 'your_folder_path' with the actual path to your folder
    folder_path = 'D:/T-drive Taxi Trajectories/release/taxi_log_2008_by_id'
    read_txt_files(folder_path)
