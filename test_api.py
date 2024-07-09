import os
import requests

BASE_URL = "http://127.0.0.1:8000"

def upload_file(file_path):
    with open(file_path, 'rb') as file:
        response = requests.post(f"{BASE_URL}/upload/", files={"file": file})
        print(f"Upload {file_path}: {response.status_code} - {response.json()}")

def get_tables(filename):
    response = requests.get(f"{BASE_URL}/configuration/{filename}/tables")
    print(f"Get tables for {filename}: {response.status_code} - {response.json()}")

def main():
    data_dir = "./data"
    for file_name in os.listdir(data_dir):
        if file_name.endswith(".yml"):
            file_path = os.path.join(data_dir, file_name)
            upload_file(file_path)
            get_tables(file_name)

if __name__ == "__main__":
    main()
