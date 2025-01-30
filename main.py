import pandas as pd
from fuzzywuzzy import fuzz
import os

# File path constants
RESPONSES_FILE = 'spreadsheets/responses.csv'
REGISTRY_FILE = 'spreadsheets/registry.xlsx'
SURVEILLANCE_FILE = 'spreadsheets/surveillance.xlsx'






# Column name constant
COLUMN_KEY = 'NAME OF FACILITY'

def is_similar_name(name1, name2, threshold=80):
    """
    Check if two names are similar using multiple fuzzy string matching methods.
    Returns True if the name parts match closely enough.
    """
    # Handle missing or non-string values
    if pd.isna(name1) or pd.isna(name2):
        return False
    
    # Convert both names to lowercase for better matching
    name1 = str(name1).lower().strip()
    name2 = str(name2).lower().strip()
    
    # Split names into parts
    parts1 = set(name1.split())
    parts2 = set(name2.split())
    
    # If the number of name parts is different by more than 1, consider it a different name
    if abs(len(parts1) - len(parts2)) > 1:
        return False
    
    # Compare each part of the names
    best_matches = []
    for part1 in parts1:
        # Find the best matching part in name2
        best_match = max((fuzz.ratio(part1, part2) for part2 in parts2), default=0)
        best_matches.append(best_match)
    
    # If we have matches for each part and they're all good matches
    return len(best_matches) > 0 and min(best_matches) >= threshold

def read_file_safe(file_path, header:int=0):
    """
    Safely read an Excel or CSV file, trying different approaches if the first one fails.
    Returns DataFrame and sheet name used (None for CSV), or (None, None) if reading fails.
    """
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return None, None

    # Handle CSV files
    if file_path.lower().endswith('.csv'):
        try:
            df = pd.read_csv(file_path)
            return df, None
        except Exception as e:
            print(f"Error reading CSV file {file_path}:")
            print(f"Error: {str(e)}")
            return None, None

    # Handle Excel files
    try:
        df = pd.read_excel(file_path, header=header)
        return df, 'default'
    except Exception as e1:
        try:
            xls = pd.ExcelFile(file_path)
            sheet_name = xls.sheet_names[0]
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=header)
            return df, sheet_name
        except Exception as e2:
            print(f"Error reading Excel file {file_path}:")
            print(f"First attempt error: {str(e1)}")
            print(f"Second attempt error: {str(e2)}")
            return None, None

def remove_duplicates(similarity_threshold=75):
    """
    Remove entries from responses.csv that have similar names in registry.xlsx
    or surveillance.xlsx. Uses fuzzy string matching to handle spelling variations.
    Saves results in Excel format.
    """
    # Read all three files safely
    results = {
        'responses': read_file_safe(RESPONSES_FILE),
        'registry': read_file_safe(REGISTRY_FILE, header=2),
        'surveillance': read_file_safe(SURVEILLANCE_FILE, header=12)
    }
    
    # Check if any file failed to load
    failed_files = [name for name, (df, _) in results.items() if df is None]
    if failed_files:
        print(f"Error: Could not read the following files: {', '.join(failed_files)}")
        return
    
    # Unpack the results
    df_responses, _ = results['responses']  # CSV files don't have sheets
    df_registry, reg_sheet = results['registry']
    df_surveillance, surv_sheet = results['surveillance']
    
    # Verify the key column exists in all files
    missing_columns = []
    for name, (df, _) in results.items():
        if COLUMN_KEY not in df.columns:
            missing_columns.append(name)
            print(f"\nColumns in {name} file:")
            print(df.columns.tolist())
    
    if missing_columns:
        print(f"\nError: '{COLUMN_KEY}' column not found in: {', '.join(missing_columns)}")
        return
    
    # Get all names from registry and surveillance
    registry_names = df_registry[COLUMN_KEY].tolist()
    surveillance_names = df_surveillance[COLUMN_KEY].tolist()
    all_names_to_check = registry_names + surveillance_names
    
    # Keep track of removed names and their matches
    removed_names = []
    matches_found = []
    removed_rows = []
    
    # Check each response name against all names in registry and surveillance
    for idx, row in df_responses.iterrows():
        response_name = row[COLUMN_KEY]
        for check_name in all_names_to_check:
            if is_similar_name(response_name, check_name, similarity_threshold):
                removed_names.append(response_name)
                matches_found.append(check_name)
                removed_rows.append(row)
                break
    
    # Create DataFrame of removed facilities with their matches
    if removed_rows:
        df_removed = pd.DataFrame(removed_rows)
        df_removed['Matched With'] = matches_found
        removed_file = 'spreadsheets/removed_facilities.xlsx'
        df_removed.to_excel(removed_file, sheet_name='Removed Facilities', index=False)
        print(f"\nRemoved facilities saved to: {removed_file}")
    
    # Remove the similar names from responses
    df_responses_filtered = df_responses[~df_responses[COLUMN_KEY].isin(removed_names)]
    
    # Print which names were removed and their matches
    if removed_names:
        print(f"\nRemoved the following {COLUMN_KEY}s:")
        for name, match in zip(removed_names, matches_found):
            print(f"- {name} (matched with: {match})")
    
    # Create backup of original file
    backup_file = RESPONSES_FILE.replace('.csv', '_backup.csv')
    df_responses.to_csv(backup_file, index=False)
    print(f"\nBackup created: {backup_file}")
    
    # Save the filtered responses as Excel
    output_file = RESPONSES_FILE.replace('.csv', '_filtered.xlsx')
    df_responses_filtered.to_excel(output_file, sheet_name='Filtered Responses', index=False)
    print(f"\nFiltered responses saved to: {output_file}")
    
    # Print statistics
    print(f"\nTotal removed: {len(removed_names)} similar {COLUMN_KEY}s")
    print(f"Filtered responses file contains {len(df_responses_filtered)} entries")

if __name__ == "__main__":
    print(f"Checking for similar {COLUMN_KEY}s in Excel files...")
    remove_duplicates()
