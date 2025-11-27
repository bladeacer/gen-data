import csv
import os
import sys
import random
import re
from faker import Faker
from multiprocessing import Pool, cpu_count
from collections import defaultdict

# --- Configuration ---
NUM_RECORDS_TO_ADD = 0
USER_FILE = 'credit_scores.csv'
ACCOUNT_FILE = 'account_status.csv'
# Increased processes for better parallel writing of 9k records
NUM_PROCESSES = min(4, cpu_count() - 1) if cpu_count() > 1 else 1 

fake = Faker('en_UK')

ACCOUNT_STATUSES = ['good-standing', 'delinquent', 'closed']
REALISTIC_DOMAINS = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com', 'icloud.com', 'live.com', 'mail.com']


# FIX 1: Corrected Header Mapping (with spaces)
USER_FIELDS = ['ID', 'Name', 'Email', 'Credit Score']
ACCOUNT_FIELDS = ['ID', 'Name', 'Email', 'Account Status']

# --- Integrity Check Function ---

def check_name_integrity(user_rows: list, account_rows: list):
    """
    Checks if the 'Name' associated with each 'ID' is consistent 
    between the credit score (user) and account status files.
    """
    
    # 1. Map all IDs to their names in the User file
    user_name_map = {}
    for row in user_rows:
        try:
            user_name_map[int(row['ID'])] = row['Name']
        except (ValueError, KeyError):
            continue # Skip invalid rows
            
    inconsistencies = []
    
    # 2. Check the Account file against the User map
    for row in account_rows:
        try:
            record_id = int(row['ID'])
            account_name = row['Name']
            
            if record_id in user_name_map:
                user_name = user_name_map[record_id]
                
                # Check for Name Mismatch
                if user_name != account_name:
                    inconsistencies.append({
                        'ID': record_id,
                        'File': ACCOUNT_FILE,
                        'User Name': user_name,
                        'Account Name': account_name,
                        'Issue': 'Name Mismatch'
                    })
                # Remove from map after checking to track unmatched IDs
                del user_name_map[record_id]
                
        except (ValueError, KeyError):
            continue
            
    # 3. Check for IDs present in User file but missing in Account file
    for record_id, user_name in user_name_map.items():
         inconsistencies.append({
             'ID': record_id,
             'File': USER_FILE,
             'User Name': user_name,
             'Account Name': 'N/A',
             'Issue': 'Missing in Account File'
         })
         
    if inconsistencies:
        print("\n❌ NAME INTEGRITY VIOLATION DETECTED ❌")
        for item in inconsistencies:
            print(f"ID: {item['ID']} | Issue: {item['Issue']} | User Name: {item['User Name']} | Account Name: {item['Account Name']}")
        # In a production environment, you might raise an exception here.
        # For data generation, we'll just log the warning.
    else:
        print("✅ Name and ID integrity check passed for existing records.")


# --- Data Generation and ID Logic ---

def read_all_data(filename: str) -> tuple[list, set]:
    """Reads all existing data and extracts all existing IDs from a single file."""
    existing_rows = []
    existing_ids = set()
    
    if not os.path.exists(filename):
        return existing_rows, existing_ids
    
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as f:
            header = f.readline().strip()
            cleaned_header = re.sub(r',+$', '', header) 
            f.seek(0)
            
            reader = csv.DictReader(f)
            
            for row in reader:
                row = {k.strip(): v.strip() for k, v in row.items() if k is not None and k.strip() != ''}
                
                try:
                    current_id = int(row['ID'])
                    existing_ids.add(current_id)
                    existing_rows.append(row)
                except (ValueError, KeyError):
                    continue
            
    except Exception:
        pass
        
    return existing_rows, existing_ids


def find_new_ids(num_records: int, combined_existing_ids: set) -> list:
    """Finds available IDs based on the combined set, prioritizing gaps."""
    
    if not combined_existing_ids:
        return list(range(1, num_records + 1))

    max_id = max(combined_existing_ids)
    
    gap_ids = [i for i in range(1, max_id) if i not in combined_existing_ids]
    new_ids = gap_ids[:num_records]
    
    if len(new_ids) < num_records:
        start_sequential = max_id + 1
        num_sequential = num_records - len(new_ids)
        new_ids.extend(range(start_sequential, start_sequential + num_sequential))
        
    new_ids.sort()
    return new_ids


def generate_new_rows(num_records: int, new_ids: list) -> tuple[list, list]:
    """Generates user and account rows using the pre-determined sorted IDs."""
    
    user_rows = []
    account_rows = []

    print(f"\nGenerating {num_records} new records...")
    
    for i, record_id in enumerate(new_ids):
        
        if (i + 1) % 1000 == 0 or (i + 1) == num_records: # Updated progress bar increment
            sys.stdout.write(f'\rProgress: {i + 1}/{num_records} records generated...')
            sys.stdout.flush() 
        
        # Realistic Email Format
        name_parts = fake.name().split()
        first_name = name_parts[0].lower()
        
        local_part_options = [
            first_name,
            f"{first_name}{random.randint(10, 99)}",
            f"{first_name}.{name_parts[-1].lower()}" if len(name_parts) > 1 else first_name,
            fake.user_name()
        ]
        local_part = random.choice(local_part_options)
        
        user_name_title = " ".join(part.title() for part in name_parts)
        domain = random.choice(REALISTIC_DOMAINS)
        user_email = f"{local_part}@{domain}"
        
        # --- Critical: Generate BOTH rows using the IDENTICAL Name ---
        user_data = {
            'ID': record_id,
            'Name': user_name_title, # The canonical Name
            'Email': user_email,
            'Credit Score': random.randint(300, 850)
        }
        
        account_data = {
            'ID': record_id, 
            'Name': user_name_title, # The canonical Name (must match user_data)
            'Email': user_email,
            'Account Status': fake.random_element(ACCOUNT_STATUSES)
        }
        
        user_rows.append(user_data)
        account_rows.append(account_data)
        
    sys.stdout.write('\rProgress: Done!                                   \n')
    return user_rows, account_rows

# --- Multiprocessing and I/O ---

def rewrite_csv_task(args):
    """Worker function for multiprocessing pool to REWRITE and sort a single file."""
    filename, fieldnames, all_rows = args
    
    all_rows.sort(key=lambda row: int(row['ID']))
    
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
    
    # --- 1. Read and Calculate IDs for both files (SYNCHRONIZED CHECK) ---
    print("--- Synchronizing and Checking existing data ---")
    existing_user_rows, user_ids = read_all_data(USER_FILE)
    existing_account_rows, account_ids = read_all_data(ACCOUNT_FILE)
    
    # Run the integrity check on the existing data before merging new records
    check_name_integrity(existing_user_rows, existing_account_rows)
    
    combined_ids = user_ids.union(account_ids)
    new_ids = find_new_ids(num_records, combined_ids)
    
    # --- 2. Generate new rows ---
    new_user_rows, new_account_rows = generate_new_rows(
        num_records, new_ids
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
    print(f"Added {len(new_ids)} denormalized records.")
    print(f"Files are now fully sorted by ID, and IDs are synchronized.")


if __name__ == '__main__':
    generate_and_append_datasets(NUM_RECORDS_TO_ADD)
