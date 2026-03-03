import requests
import csv
import os
import time
import concurrent.futures

# --- Configuration ---
REGISTRY_URL = "https://raw.githubusercontent.com/Eliasdegemu61/Sodex-Tracker-new-v1/refs/heads/main/registry.json"
API_URL = "https://mainnet-data.sodex.dev/api/v1/spot/trades"
CSV_FILE = "spot_vol.csv"
POINTER_FILE = "pointer.txt"
TEN_K_BATCH = 10000 
MAX_WORKERS = 4
LIMIT = 1000

session = requests.Session()

def get_pointer():
    """Reads where the last run left off."""
    if os.path.exists(POINTER_FILE):
        with open(POINTER_FILE, 'r') as f:
            content = f.read().strip()
            return int(content) if content else 0
    return 0

def save_pointer(val):
    """Saves the index for the next run."""
    with open(POINTER_FILE, 'w') as f:
        f.write(str(val))

def load_existing_data():
    """Loads current volume and timestamps from CSV."""
    data = {}
    if os.path.exists(CSV_FILE):
        try:
            with open(CSV_FILE, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    data[row['user_id']] = {
                        'address': row['address'],
                        'vol': float(row['vol']),
                        'last_ts': int(row['last_ts'])
                    }
        except Exception as e:
            print(f"⚠️ CSV Load Warning: {e}")
    return data

def save_to_csv(full_data):
    """Sorts by volume and saves to CSV."""
    sorted_items = sorted(full_data.items(), key=lambda x: x[1]['vol'], reverse=True)
    with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['user_id', 'address', 'vol', 'last_ts'])
        for uid, info in sorted_items:
            writer.writerow([uid, info['address'], info['vol'], info['last_ts']])

def fetch_user_incremental(user, existing_info):
    """Fetches new trades since last_ts and calculates volume."""
    uid = str(user['userId'])
    address = user['address']
    current_vol = existing_info.get('vol', 0.0)
    stop_ts = existing_info.get('last_ts', 0)
    
    new_last_ts = None
    added_vol = 0.0
    cursor = None
    
    while True:
        try:
            params = {"account_id": uid, "limit": LIMIT}
            if cursor: params["cursor"] = cursor
            
            resp = session.get(API_URL, params=params, timeout=25)
            if resp.status_code == 429:
                time.sleep(10)
                continue
            
            resp.raise_for_status()
            res_json = resp.json()
            trades = res_json.get("data", [])
            meta = res_json.get("meta", {})

            if not trades:
                break

            # Capture the very first trade's TS as the new bookmark
            if new_last_ts is None:
                new_last_ts = trades[0]['ts_ms']

            reached_old = False
            for t in trades:
                ts = int(t['ts_ms'])
                # Stop if we hit or pass the old last_ts
                if ts <= stop_ts:
                    reached_old = True
                    break
                added_vol += float(t.get('price', 0)) * float(t.get('quantity', 0))

            if reached_old:
                break

            cursor = meta.get("next_cursor")
            if not cursor:
                break
        except Exception as e:
            print(f"  Err {uid}: {e}")
            break

    if added_vol > 0:
        print(f"   [DATA] User {uid}: +${added_vol:,.2f}")

    return uid, {
        'address': address,
        'vol': current_vol + added_vol,
        'last_ts': new_last_ts if new_last_ts else stop_ts
    }

def trigger_next_run():
    """Triggers the next batch via Repository Dispatch."""
    token = os.getenv("GH_TOKEN")
    repo = os.getenv("REPO")
    if not token or not repo:
        print("⚠️ GH_TOKEN or REPO env not found. Auto-loop disabled.")
        return

    url = f"https://api.github.com/repos/{repo}/dispatches"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json"
    }
    data = {"event_type": "start-next-batch"}
    
    # Wait for GitHub to finish the git push internal processing
    time.sleep(15) 
    
    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 204:
        print("🚀 Successfully triggered the next 10k batch!")
    else:
        print(f"❌ Failed to trigger loop: {response.status_code} - {response.text}")

def main():
    print("🚀 Fetching Registry...")
    try:
        registry = requests.get(REGISTRY_URL).json()
    except Exception as e:
        print(f"Fatal Registry Error: {e}")
        return

    all_data = load_existing_data()
    start_index = get_pointer()
    
    # RESET: If at the end, go back to 0
    if start_index >= len(registry):
        print("🔄 End of Registry. Restarting cycle from 0.")
        start_index = 0

    end_index = start_index + TEN_K_BATCH
    current_chunk = registry[start_index : end_index]
    
    print(f"▶️ BATCH START: Users {start_index} to {min(end_index, len(registry))}")
    
    temp_results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_user_incremental, u, all_data.get(str(u['userId']), {})): u 
            for u in current_chunk
        }
        
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            uid, res = future.result()
            temp_results[uid] = res
            if i % 1000 == 0:
                print(f"   Progress: {i}/{len(current_chunk)} processed...")

    # Merge results and save
    all_data.update(temp_results)
    save_to_csv(all_data)
    save_pointer(end_index)
    print(f"✅ Chunk saved. Pointer now at {end_index}.")

if __name__ == "__main__":
    main()
    trigger_next_run()
