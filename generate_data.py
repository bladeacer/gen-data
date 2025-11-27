import csv
import os
from faker import Faker
import random

NUM_RECORDS_TO_ADD = 50
USER_FILE = './credit_scores.csv'
ACCOUNT_FILE = './account_status.csv'

fake = Faker('en_UK')

ACCOUNT_STATUSES = ['good-standing', 'delinquent', 'closed']

USER_FIELDS = ['id', 'name', 'email', 'credit_score']

ACCOUNT_FIELDS = ['id', 'name', 'email', 'account_status']

def get_next_available_id(filename: str, id_field_name: str) -> int:
    """
    Finds the smallest available non-existent ID by checking for gaps 
    in the existing sequence.
    """
    if not os.path.exists(filename):
        return 1
    
    existing_ids = set()
    try:
        with open(filename, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            # 1. Read all existing IDs into a set for fast lookup
            for row in reader:
                try:
                    existing_ids.add(int(row[id_field_name]))
                except ValueError:
                    # Skip rows where ID field is missing or not an integer
                    continue
        
    except Exception as e:
        print(f"Error reading IDs from {filename}: {e}. Starting ID from 1.")
        return 1

    if not existing_ids:
        return 1

    # Find the maximum existing ID
    max_id = max(existing_ids)
    
    # 2. Iterate from the smallest possible ID (1) up to the max ID
    # This finds the first missing integer (the smallest gap)
    for i in range(1, max_id):
        if i not in existing_ids:
            print(f"Found smallest gap ID: {i}. Will use this ID.")
            return i
            
    # 3. If no gaps are found (i.e., the sequence is complete), 
    # start from the next sequential number after the max ID.
    return max_id + 1

def generate_and_append_datasets(num_records: int):
    """
    Generates and appends two DENORMALIZED, linked datasets.
    """
    starting_id = get_next_available_id(USER_FILE, 'id')
    
    user_rows = []
    account_rows = []

    for i in range(num_records):
        record_id = starting_id + i
        
        user_name = fake.name()
        user_email = fake.email()

        user_rows.append({
            'id': record_id,
            'name': user_name,
            'email': user_email,
            'credit_score': random.randint(300, 850)
        })

        account_rows.append({
            'id': record_id, 
            'name': user_name,
            'email': user_email,
            'account_status': fake.random_element(ACCOUNT_STATUSES)
        })
        
    append_to_csv(USER_FILE, SYSTEM_A_FIELDS, user_rows)
    append_to_csv(ACCOUNT_FILE, SYSTEM_B_FIELDS, account_rows)

    print(f"\n--- Generation Summary ---")
    print(f"Added {num_records} denormalized records.")
    print(f"User IDs range: {starting_id} to {starting_id + num_records - 1}")

def append_to_csv(filename: str, fieldnames: list, rows: list):
    """Generic function to write rows to a CSV, creating headers if the file is new."""
    file_exists = os.path.exists(filename)
    
    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if not file_exists:
            writer.writeheader()
            print(f"Created new file: '{filename}' with headers.")
        
        writer.writerows(rows)
        
    print(f"Data appended to '{filename}'.")


if __name__ == '__main__':
    generate_and_append_datasets(NUM_RECORDS_TO_ADD)
