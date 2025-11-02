import os

RAW_DIR = "data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

def download_dataset(name, dataset_name, target_dir):
    print(f"Attempting to download {name} dataset from Kaggle...")
    os.system(f'kaggle datasets download -d {dataset_name} -p {target_dir} --unzip')
    print(f"{name} dataset saved to {target_dir}")

def main():
    download_dataset("Netflix", "shivamb/netflix-shows", RAW_DIR)
    download_dataset("Amazon", "shivamb/amazon-prime-movies-and-tv-shows", RAW_DIR)

if __name__ == "__main__":
    main()

