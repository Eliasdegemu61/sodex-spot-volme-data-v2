import requests
import csv
import os
import time
import concurrent.futures

# Config
REGISTRY_URL = "https://raw.githubusercontent.com/Eliasdegemu61/Sodex-Tracker-new-v1/refs/heads/main/registry.json"
API_URL = "https://mainnet-data.sodex.dev/api/v1/spot/trades"
CSV_FILE = "spot_vol.csv"
POINTER_FILE = "pointer.txt"
TEN_K_BATCH = 10000 
MAX_WORKERS = 4
LIMIT = 1000

session = requests.Session()

def get_pointer():
    if os.path.exists(POINTER_FILE):
        with open(POINTER_FILE, 'r') as f:
            return int(f.read().strip())
    return 0

def save_pointer(val):
    with open(POINTER_FILE, 'w') as f:
        f.write(str(val))

def load_existing_data():
    data = {}
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data[row['user_id']] = {
                    'address': row['address'],
                    'vol': float(row['vol']),
                    'last_ts': int(row['last_ts'])
                }
    return data

def save_to_csv(full_data):
    sorted_items = sorted(full_data.items(), key=lambda x: x[1]['vol'], reverse=True)
    with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['user_id', 'address', 'vol', 'last_ts'])
        for uid, info in sorted_items:
            writer.writerow([uid, info['address'], info['vol'], info['last_ts']])

def fetch_user_incremental(user, existing_info):
    uid = str(user['userId'])
    address = user['address']
    current_vol = existing_info.get('vol', 0.0)
    stop_ts = existing_info.get('last_ts', 0)
    new_last_ts, added_vol, cursor = None, 0.0, None
    
    while True:
        try:
            params = {"account_id": uid, "limit": LIMIT}
            if cursor: params["cursor"] = cursor
            resp = session.get(API_URL, params=params, timeout=25)
            if resp.status_code == 429:
                time.sleep(10); continue
            resp.raise_for_status()
            trades = resp.json().get("data", [])
            meta = resp.json().get("meta", {})
            if not trades: break
            if new_last_ts is None: new_last_ts = trades[0]['ts_ms']
            reached_old = False
            for t in trades:
                if int(t['ts_ms']) <= stop_ts:
                    reached_old = True; break
                added_vol += float(t.get('price', 0)) * float(t.get('quantity', 0))
            if reached_old: break
            cursor = meta.get("next_cursor")
            if not cursor: break
        except Exception: break

    return uid, {'address': address, 'vol': current_vol + added_vol, 'last_ts': new_last_ts if new_last_ts else stop_ts}

def main():
    registry = requests.get(REGISTRY_URL).json()
    all_data = load_existing_data()
    start_index = get_pointer()
    
    # If the pointer is beyond the registry, reset to 0 (Start new cycle)
    if start_index >= len(registry):
        print("🔄 End of registry reached. Restarting from User 0.")
        start_index = 0

    end_index = start_index + TEN_K_BATCH
    current_chunk = registry[start_index : end_index]
    
    print(f"▶️ RUNNING CYCLE: Users {start_index} to {min(end_index, len(registry))}")
    
    temp_results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_user_incremental, u, all_data.get(str(u['userId']), {})): u for u in current_chunk}
        for i, future in enumerate(concurrent.futures.as_completed(futures), 1):
            uid, res = future.result()
            temp_results[uid] = res
            if i % 100 == 0: print(f"   Progress: {i}/{len(current_chunk)} users processed...")

    all_data.update(temp_results)
    save_to_csv(all_data)
    
    # Update pointer for the NEXT cron job
    save_pointer(end_index)
    print(f"✅ Saved results. Next run will start at index: {end_index}")

if __name__ == "__main__":
    main()
