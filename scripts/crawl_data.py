import requests
import pandas as pd
from datetime import datetime
import time
import os

BASE_LIST_URL = "https://gateway.chotot.com/v1/public/ad-listing"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.vieclamtot.com/"
}

def convert_timestamp_to_iso(timestamp_ms):
    if not timestamp_ms:
        return datetime.now().isoformat()
    ts = timestamp_ms // 1000
    return datetime.fromtimestamp(ts).isoformat()

def scrape_jobs(total=5000, limit=50):
    results = []
    pages = total // limit + 1

    for page in range(1, pages + 1):
        params = {"page": page, "limit": limit, "cg": "13010"}
        print(f"Fetching page {page}/{pages} ...")
        try:
            r = requests.get(BASE_LIST_URL, headers=HEADERS, params=params, timeout=10)
            r.raise_for_status()  # Raise an exception for bad status codes
            ads = r.json().get("ads", [])
            if not ads:
                print("No more ads, stopping.")
                break
            for ad in ads:
                ad["timestamp_iso"] = convert_timestamp_to_iso(ad.get("list_time"))
                results.append(ad)
            if len(results) >= total:
                break
            time.sleep(1)
        except requests.exceptions.RequestException as e:
            print(f"Failed page {page}, error: {e}")
            continue
    return results

# === Main execution ===
if __name__ == "__main__":
    jobs = scrape_jobs(total=5000, limit=50)

    # Path to the data directory, relative to this script's location
    DATA_DIR = 'data'
    
    # Create data directory if it doesn't exist
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    df = pd.DataFrame(jobs)
    
    # Update paths to save files in the data directory
    csv_path = os.path.join(DATA_DIR, 'jobs.csv')
    json_path = os.path.join(DATA_DIR, 'jobs.json')
    
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_json(json_path, orient="records", force_ascii=False)

    print(f"\nScraped {len(df)} jobs and saved to {csv_path} and {json_path}")