import os
import requests

DATASETS = {
    "netflix": "https://www.kaggle.com/datasets/shivamb/netflix-shows/download",
    "amazon": "https://www.kaggle.com/datasets/shivamb/amazon-prime-movies-and-tv-shows/download"
}

RAW_DIR = "data/raw"

def download_url(name, url, target_file):
    os.makedirs(RAW_DIR, exist_ok=True)
   
    print(f"Attempting to download {name} dataset from {url}")
    
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(target_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f" {name} dataset saved to {target_file}")
    else:
        print(f" Couldn't download {name} automatically. Please download manually from Kaggle and place in {target_file}")

def main():
    download_url("netflix", DATASETS["netflix"], os.path.join(RAW_DIR, "netflix_titles.csv"))
    download_url("amazon", DATASETS["amazon"], os.path.join(RAW_DIR, "amazon_prime_titles.csv"))

if __name__ == "__main__":
    main()
