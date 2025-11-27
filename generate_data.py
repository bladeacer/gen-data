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
CHUNK_SIZE = 100
# Increased processes for better parallel writing of 9k records
NUM_PROCESSES = min(4, cpu_count() - 1) if cpu_count() > 1 else 1 

fake = Faker('en_UK')

ACCOUNT_STATUSES = ['good-standing', 'delinquent', 'closed']
REALISTIC_DOMAINS = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com', 'icloud.com', 'live.com', 'mail.com']


# FIX 1: Corrected Header Mapping (with spaces)
USER_FIELDS = ['ID', 'Name', 'Email', 'Credit Score']
ACCOUNT_FIELDS = ['ID', 'Name', 'Email', 'Account Status']

CLEAN_USER_FILE = 'credit_scores_clean.csv'
CLEAN_ACCOUNT_FILE = 'account_status_clean.csv'
CLEAN_USER_FIELDS = ['Name', 'Credit Score']
CLEAN_ACCOUNT_FIELDS = ['Name', 'Account Status']

def write_clean_csv(base_filename: str, clean_fieldnames: list, all_rows: list):
    """
    Writes data to multiple clean CSV files, splitting the total dataset
    into chunks of CHUNK_SIZE (100 rows).
    """
    total_records = len(all_rows)
    CHUNK_SIZE = 100 # Define the chunk size inside the function or use the constant

    print(f"Starting to write {total_records} records to multiple files (Chunk Size: {CHUNK_SIZE})...")
    
    # 1. Loop through all_rows in steps of CHUNK_SIZE
    for i in range(0, total_records, CHUNK_SIZE):
        
        # Determine the start and end indices for the current chunk
        chunk = all_rows[i:i + CHUNK_SIZE]
        
        # Determine the file index (1-based) and construct the unique filename
        file_index = (i // CHUNK_SIZE) + 1
        
        # Example: 'credit_scores_clean.csv' -> 'credit_scores_clean_part_1.csv'
        filename, extension = os.path.splitext(base_filename)
        output_filename = f"{filename}_part_{file_index}{extension}"
        
        try:
            with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(
                    csvfile, 
                    fieldnames=clean_fieldnames,
                    extrasaction='ignore' # Ensures 'ID' and 'Email' are ignored
                )
                
                writer.writeheader()
                writer.writerows(chunk)
            
            print(f"  -> Wrote {len(chunk)} records to **{output_filename}**")
            
        except Exception as e:
            print(f"Error writing chunk file {output_filename}: {e}")

    print(f"✅ Finished writing all clean CSV chunks based on **{base_filename}**.")

# --- Integrity Check Function ---

def check_data_integrity(user_rows: list, account_rows: list):
    """
    Checks if the 'Name' and 'Email' associated with each 'ID' are consistent 
    between the credit score (user) and account status files.
    """
    
    # 1. Map all IDs to their Name and Email in the User file
    user_data_map = {}
    for row in user_rows:
        try:
            record_id = int(row['ID'])
            user_data_map[record_id] = {
                'Name': row['Name'],
                'Email': row['Email']
            }
        except (ValueError, KeyError):
            continue # Skip rows that passed read_all_data but are missing name/email
            
    inconsistencies = []
    
    # 2. Check the Account file against the User map
    for row in account_rows:
        try:
            record_id = int(row['ID'])
            account_name = row['Name']
            account_email = row['Email']
            
            if record_id in user_data_map:
                user_name = user_data_map[record_id]['Name']
                user_email = user_data_map[record_id]['Email']
                
                # Check 2A: Name Mismatch
                if user_name != account_name:
                    inconsistencies.append({
                        'ID': record_id,
                        'Field': 'Name',
                        'User Value': user_name,
                        'Account Value': account_name,
                        'Issue': 'Name Mismatch'
                    })
                
                # Check 2B: Email Mismatch (THE NEW CHECK)
                if user_email != account_email:
                    inconsistencies.append({
                        'ID': record_id,
                        'Field': 'Email',
                        'User Value': user_email,
                        'Account Value': account_email,
                        'Issue': 'Email Mismatch'
                    })

                # Remove from map after checking to track unmatched IDs
                del user_data_map[record_id]
                
        except (ValueError, KeyError):
            continue
            
    # 3. Check for IDs present in User file but missing in Account file
    for record_id, data in user_data_map.items():
         inconsistencies.append({
             'ID': record_id,
             'Field': 'ID',
             'User Value': data['Name'],
             'Account Value': 'N/A',
             'Issue': f'Missing match for ID {record_id} in other file.'
         })
         
    if inconsistencies:
        print("\n❌ DATA INTEGRITY VIOLATION DETECTED ❌")
        for item in inconsistencies:
            print(f"ID: {item['ID']} | Field: {item['Field']} | Issue: {item['Issue']} | User: {item['User Value']} | Account: {item['Account Value']}")
    else:
        print("✅ ID, Name, and Email integrity check passed for existing records.")


# --- Data Generation and ID Logic ---

def read_all_data(filename: str) -> tuple[list, set, list]:
    """
    Reads all existing data, extracting IDs, and REPORTS invalid rows.
    Returns: (existing_rows, existing_ids, invalid_rows)
    """
    existing_rows = []
    existing_ids = set()
    invalid_rows = [] # New list to store invalid rows
    
    if not os.path.exists(filename):
        return existing_rows, existing_ids, invalid_rows
    
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as f:
            header = f.readline().strip()
            cleaned_header = re.sub(r',+$', '', header) 
            f.seek(0)
            
            reader = csv.DictReader(f)
            
            for row in reader:
                # Clean up any potential extra empty keys/whitespace
                row = {k.strip(): v.strip() for k, v in row.items() if k is not None and k.strip() != ''}
                
                try:
                    # Attempt to read and validate the ID
                    current_id = int(row.get('ID', ''))
                    existing_ids.add(current_id)
                    existing_rows.append(row)
                except (ValueError, KeyError) as e:
                    # Report the row instead of skipping
                    reason = "Missing ID" if 'ID' not in row else "Non-integer ID"
                    invalid_rows.append({'file': filename, 'row_data': row, 'reason': reason})
                    continue
            
    except Exception as e:
        # Report file-level errors (e.g., bad file format)
        print(f"File reading error for {filename}: {e}")
        
    return existing_rows, existing_ids, invalid_rows


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
    
    # Updated call to read_all_data to capture invalid rows
    existing_user_rows, user_ids, user_invalid_rows = read_all_data(USER_FILE)
    existing_account_rows, account_ids, account_invalid_rows = read_all_data(ACCOUNT_FILE)
    
    # Report Invalid Rows
    all_invalid_rows = user_invalid_rows + account_invalid_rows
    if all_invalid_rows:
        print(f"\n⚠️ WARNING: {len(all_invalid_rows)} INVALID ROWS DETECTED and SKIPPED ⚠️")
        for item in all_invalid_rows:
            # Print a concise representation of the invalid data
            print(f"File: {item['file']} | Reason: {item['reason']} | Data Sample: {str(item['row_data'])[:100]}...")
    
    # Run the integrity check on the *valid* existing data
    check_data_integrity(existing_user_rows, existing_account_rows)
    
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

    print("\n--- Generating Clean CSV Files (Name and Data Field, chunks of 100) ---")
    write_clean_csv(CLEAN_USER_FILE, CLEAN_USER_FIELDS, all_user_rows)
    write_clean_csv(CLEAN_ACCOUNT_FILE, CLEAN_ACCOUNT_FIELDS, all_account_rows)

    print(f"\n--- Generation Summary ---")
    print(f"Added {len(new_ids)} denormalized records.")
    print(f"Files are now fully sorted by ID, and IDs are synchronized.")


if __name__ == '__main__':
    generate_and_append_datasets(NUM_RECORDS_TO_ADD)
