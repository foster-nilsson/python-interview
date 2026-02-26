import os
import json
import re
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional, Tuple

# Configuration
API_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
DATA_DIR = "data"
METADATA_FILE = "metadata.json"
THEME_FILTER = "Hospitals"

def to_snake_case(text: str) -> str:
    """
    Converts a string to snake_case.
    Removes special characters, converts spaces to underscores, and lowercases everything.
    Example: "Patients’ rating of the facility linear mean score" 
    becomes "patients_rating_of_the_facility_linear_mean_score"
    """
    # Remove apostrophes specifically to match example ("Patients’" -> "patients")
    # Using both standard and curly apostrophes
    s = text.replace("’", "").replace("'", "")
    # Replace non-alphanumeric characters with spaces
    s = re.sub(r'[^a-zA-Z0-9\s]', ' ', s)
    # Replace one or more spaces with a single underscore
    s = re.sub(r'\s+', '_', s.strip())
    return s.lower()

def load_metadata() -> Dict[str, str]:
    """Loads run metadata from a local JSON file."""
    if os.path.exists(METADATA_FILE):
        try:
            with open(METADATA_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            print(f"Warning: Could not read {METADATA_FILE}. Starting fresh.")
    return {}

def save_metadata(metadata: Dict[str, str]):
    """Saves updated metadata to the local JSON file."""
    with open(METADATA_FILE, 'w') as f:
        json.dump(metadata, f, indent=4)

def process_dataset(dataset: Dict, current_metadata: Dict) -> Optional[Tuple[str, str]]:
    """
    Downloads and processes a single dataset if it has been modified.
    Returns (identifier, modified_date) on success, or None.
    """
    identifier = dataset.get('identifier')
    modified_date = dataset.get('modified')
    title = dataset.get('title', identifier)

    if not identifier or not modified_date:
        return None

    # Skip if already downloaded and not modified
    if current_metadata.get(identifier) == modified_date:
        print(f"[-] Skipping: {title} (Up to date)")
        return None

    # Find the CSV distribution URL
    distribution = next((d for d in dataset.get('distribution', []) if d.get('mediaType') == 'text/csv'), None)
    if not distribution or 'downloadURL' not in distribution:
        print(f"[!] No CSV found for: {title}")
        return None

    download_url = distribution['downloadURL']
    # Use title in filename for readability, snake_cased
    safe_title = to_snake_case(title)
    file_path = os.path.join(DATA_DIR, f"{safe_title}.csv")

    print(f"[+] Downloading: {title}...")
    try:
        # Load CSV, transform headers, and save
        df = pd.read_csv(download_url)
        df.columns = [to_snake_case(col) for col in df.columns]
        df.to_csv(file_path, index=False)
        print(f"[*] Processed: {title} -> {file_path}")
        return identifier, modified_date
    except Exception as e:
        print(f"[!] Error processing {title}: {e}")
        return None

def main():
    """Main execution block."""
    print("--- CMS Hospital Data Downloader ---")
    
    # Ensure data directory exists
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    # Load previous run metadata
    metadata = load_metadata()
    
    print(f"Fetching metadata from {API_URL}...")
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        datasets = response.json()
    except Exception as e:
        print(f"Failed to fetch API data: {e}")
        return

    # Filter for "Hospitals" theme
    hospital_datasets = [d for d in datasets if THEME_FILTER in d.get('theme', [])]
    print(f"Found {len(hospital_datasets)} datasets matching theme '{THEME_FILTER}'.")

    # Process datasets in parallel
    # Using 5 threads to be respectful but efficient
    with ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(lambda d: process_dataset(d, metadata), hospital_datasets))

    # Update metadata with successful runs
    updated = False
    for res in results:
        if res:
            identifier, modified_date = res
            metadata[identifier] = modified_date
            updated = True

    if updated:
        save_metadata(metadata)
        print("\nIncremental run complete. Metadata updated.")
    else:
        print("\nNo new or modified data identified.")

if __name__ == "__main__":
    main()
