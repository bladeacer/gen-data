import csv
import os
import sys
import random
from faker import Faker
from multiprocessing import Pool, cpu_count

# --- Configuration ---
NUM_RECORDS_TO_ADD = 50
USER_FILE = 'credit_scores.csv'
ACCOUNT_FILE = 'account_status.csv'
NUM_PROCESSES = min(2, cpu_count() - 1) if cpu_count() > 1 else 1 

fake = Faker('en_UK')

ACCOUNT_STATUSES = ['good-standing', 'delinquent', 'closed']

# Required Title Case Headers
USER_FIELDS = ['ID', 'Name', 'Email', 'Credit_Score']
ACCOUNT_FIELDS = ['ID', 'Name', 'Email', 'Account_Status']

# --- Key Mapping Functions (Helper functions for cleaner internal processing) ---

def map_keys_to_title_case(data_dict: dict, fields: list) -> dict:
    """Converts snake_case keys to Title Case keys for CSV writing."""
    mapping = {k.lower().replace('_', ''): k for k in fields}
    title_case_dict = {}
    for snake_key, value in data_dict.items():
        if snake_key.lower().replace('_', '') in mapping:
            title_case_dict[mapping[snake_key.lower().replace('_', '')]] = value
    return title_case_dict

# --- Data Generation and ID Logic ---

def read_all_data(filename: str) -> tuple[list, set]:
    """Reads all existing data and extracts all existing IDs from a single file."""
    existing_rows = []
    existing_ids = set()
    
    if not os.path.exists(filename):
        return existing_rows, existing_ids
    
    try:
        # Use open() with explicit newline='' to fix potential ^M (CR) issues
        with open(filename, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            id_field = 'ID' 
            
            for row in reader:
                # Clean up any potential extra empty keys/whitespace
                row = {k: v.strip() for k, v in row.items() if k is not None and k.strip() != ''}
                
                try:
                    current_id = int(row[id_field])
                    existing_ids.add(current_id)
                    existing_rows.append(row)
                except (ValueError, KeyError):
                    continue
            
    except Exception:
        # Silently fail if file is unreadable, assuming it's empty
        pass
        
    return existing_rows, existing_ids


def find_new_ids(num_records: int, combined_existing_ids: set) -> list:
    """Finds available IDs based on the combined set, prioritizing gaps."""
    
    if not combined_existing_ids:
        return list(range(1, num_records + 1))

    max_id = max(combined_existing_ids)
    
    # 1. Find all gap IDs up to the current max ID
    gap_ids = [i for i in range(1, max_id) if i not in combined_existing_ids]
    
    # 2. Determine the sequence of new IDs: gaps first
    new_ids = gap_ids[:num_records]
    
    # 3. If we still need more records, use sequential IDs
    if len(new_ids) < num_records:
        start_sequential = max_id + 1
        num_sequential = num_records - len(new_ids)
        new_ids.extend(range(start_sequential, start_sequential + num_sequential))
        
    new_ids.sort()
    return new_ids


def generate_new_rows(num_records: int, new_ids: list, user_fields: list, account_fields: list) -> tuple[list, list]:
    """Generates user and account rows using the pre-determined sorted IDs."""
    
    user_rows = []
    account_rows = []

    print(f"\nGenerating {num_records} new records...")
    
    for i, record_id in enumerate(new_ids):
        
        if (i + 1) % 10 == 0 or (i + 1) == num_records:
            sys.stdout.write(f'\rProgress: {i + 1}/{num_records} records generated...')
            sys.stdout.flush() 
        
        # Creative Email Generation (using format: first.last_ID@random-domain.co.uk)
        name_parts = fake.name().split()
        first_name = name_parts[0].lower()
        last_name = name_parts[-1].lower() if len(name_parts) > 1 else "user"
        
        user_name_title = f"{first_name.title()} {last_name.title()}"
        user_email = f"{first_name}.{last_name}_{record_id}@{fake.domain_name()}"
        
        # Data creation using Title Case keys
        user_data = {
            'ID': record_id,
            'Name': user_name_title,
            'Email': user_email,
            'Credit_Score': random.randint(300, 850)
        }
        
        account_data = {
            'ID': record_id, 
            'Name': user_name_title,
            'Email': user_email,
            'Account_Status': fake.random_element(ACCOUNT_STATUSES)
        }
        
        user_rows.append(user_data)
        account_rows.append(account_data)
        
    sys.stdout.write('\rProgress: Done!                                   \n')
    return user_rows, account_rows

# --- Multiprocessing and I/O ---

def rewrite_csv_task(args):
    """Worker function for multiprocessing pool to REWRITE and sort a single file."""
    filename, fieldnames, all_rows = args
    
    # 1. Sort ALL rows (existing + new) by the ID column (Crucial Step!)
    all_rows.sort(key=lambda row: int(row['ID']))
    
    # 2. Open in 'w' (write/overwrite) mode
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerows(all_rows)
    
    print(f"[PROCESS] Rewrote and sorted {len(all_rows)} records in {filename}.")


def rewrite_csv_parallel(file_data_list):
    """Manages the multiprocessing pool for rewriting both CSV files."""
    print(f"\nStarting parallel REWRITE and sort process with {NUM_PROCESSES} workers...")
    
    try:
        with Pool(processes=NUM_PROCESSES) as pool:
            pool.map(rewrite_csv_task, file_data_list)
            
    except Exception as e:
        print(f"An error occurred during parallel write: {e}")
        
    print("Parallel rewrite completed.")


def generate_and_append_datasets(num_records: int):
    
    # --- 1. Read and Calculate IDs for both files (THE FIX) ---
    existing_user_rows, user_ids = read_all_data(USER_FILE)
    existing_account_rows, account_ids = read_all_data(ACCOUNT_FILE)
    
    # MERGE IDs: This ensures the generated IDs are unique across BOTH files.
    combined_ids = user_ids.union(account_ids)
    
    # Calculate new IDs based on the combined set
    new_ids = find_new_ids(num_records, combined_ids)
    
    # --- 2. Generate new rows ---
    new_user_rows, new_account_rows = generate_new_rows(
        num_records, new_ids, USER_FIELDS, ACCOUNT_FIELDS
    )

    # --- 3. Combine and Prepare for Rewrite ---
    all_user_rows = existing_user_rows + new_user_rows
    all_account_rows = existing_account_rows + new_account_rows

    # Prepare data structure for the parallel rewrite function
    file_data_list = [
        (USER_FILE, USER_FIELDS, all_user_rows),
        (ACCOUNT_FILE, ACCOUNT_FIELDS, all_account_rows)
    ]
    
    # --- 4. Rewrite and Sort in Parallel ---
    rewrite_csv_parallel(file_data_list)

    print(f"\n--- Generation Summary ---")
    print(f"Added {num_records} denormalized records.")
    print(f"Files are now fully sorted by ID, and IDs are synchronized.")


if __name__ == '__main__':
    generate_and_append_datasets(NUM_RECORDS_TO_ADD)
