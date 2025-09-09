import requests
import pandas as pd
from datetime import datetime
import time

BASE_LIST_URL = "https://gateway.chotot.com/v1/public/ad-listing"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0 Safari/537.36",
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

    for page in range(1, pages+1):
        params = {"page": page, "limit": limit, "cg": "13010"}  # job category
        print(f"Fetching page {page}/{pages} ...")

        r = requests.get(BASE_LIST_URL, headers=HEADERS, params=params, timeout=10)
        if r.status_code != 200:
            print(f"Failed page {page}, status {r.status_code}")
            continue

        ads = r.json().get("ads", [])
        if not ads:
            print("No more ads, stopping.")
            break

        for ad in ads:
            ad["timestamp_iso"] = convert_timestamp_to_iso(ad.get("list_time"))
            ad["url"] = f"https://www.vieclamtot.com/viec-lam/{ad.get('list_id')}"
            results.append(ad)

        if len(results) >= total:
            break
        time.sleep(1)

    return results


# Run scraper
jobs = scrape_jobs(total=5000, limit=50)

df = pd.DataFrame(jobs)
df.to_csv("jobs.csv", index=False, encoding="utf-8-sig")
df.to_json("jobs.json", orient="records", force_ascii=False)

print(f"Scraped {len(df)} jobs")
print(df.head())