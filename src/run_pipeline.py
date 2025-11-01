import subprocess

print(" Starting full OTT Data Pipeline...")

#  Fetch raw datasets
print("\n Running data fetch (Module 1)...")
subprocess.run(["python", "src/fetch_data.py"], check=True)

#  Clean & integrate datasets
print("\n Running data cleaning (Module 2)...")
subprocess.run(["python", "src/clean_data.py"], check=True)

print("\n Pipeline completed successfully! Cleaned data saved to data/processed/ott_combined.parquet")
