"""
Data Preprocessing Script
Joins and organizes trade CSV files using heapq.merge for memory efficiency
"""
import os
import sys
import heapq
import csv
from pathlib import Path
from typing import (
    Iterator,
    Tuple,
    Dict,
    Any
)

import polars as pl
from dotenv import load_dotenv

def get_close_time_column(file_path: str) -> str:
    """Find the close time column in the CSV file"""
    close_time_columns = ['closed_at', 'close_time', 'closetime', 'close_datetime']
    
    # Read header to check columns
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
    
    for col in close_time_columns:
        if col in headers:
            return col
    
    raise ValueError(f"No close time column found in {file_path}. Available: {headers}")

def csv_row_iterator(file_path: str, sort_column: str) -> Iterator[Tuple[str, Dict[str, Any]]]:
    """Iterator that yields (sort_key, row_dict) tuples for heapq.merge"""
    from datetime import datetime
    
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert datetime string to datetime object for proper comparison
            date_str = row[sort_column]
            try:
                # Try to parse the datetime string - this ensures proper chronological sorting
                datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f")
                # If parsing succeeds, use the original string (ISO format sorts correctly)
                sort_key = date_str
            except ValueError:
                try:
                    # Try alternative format without microseconds
                    datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                    sort_key = date_str
                except ValueError:
                    # If parsing fails, use string as-is (fallback)
                    print(f"⚠️  Warning: Could not parse datetime '{date_str}', using string comparison")
                    sort_key = date_str
            
            yield (sort_key, row)

def join_and_organize_trades(
    trades_file1: str, 
    trades_file2: str, 
    output_file: str
) -> None:
    """
    Join and organize trade CSV files using heapq.merge for memory efficiency
    
    Args:
        trades_file1: Path to first trades CSV file
        trades_file2: Path to second trades CSV file  
        output_file: Path to output combined CSV file
    """
    print(f"📄 Processing trades CSV files with heapq.merge...")
    
    try:
        # Find the close time column
        print("🔍 Detecting close time column...")
        sort_column = get_close_time_column(trades_file1)
        print(f"   ✅ Using sort column: {sort_column}")
        print("   ✅ Assuming both files are already sorted by closed_at")
        
        print("🔗 Merging pre-sorted files with deduplication...")
        
        # Get headers from first file
        with open(trades_file1, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
        
        # Check if identifier column exists
        has_identifier = 'identifier' in headers
        seen_identifiers = set() if has_identifier else None
        
        # Create iterators for already-sorted files
        iter1 = csv_row_iterator(trades_file1, sort_column)
        iter2 = csv_row_iterator(trades_file2, sort_column)
        
        # Use heapq.merge to combine sorted iterators
        merged_iterator = heapq.merge(iter1, iter2, key=lambda x: x[0])
        
        # Write merged and deduplicated results
        total_records = 0
        duplicates_removed = 0
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            
            for sort_key, row in merged_iterator:
                total_records += 1
                
                # Handle deduplication if identifier column exists
                if has_identifier:
                    identifier = row['identifier']
                    if identifier in seen_identifiers:
                        duplicates_removed += 1
                        continue
                    seen_identifiers.add(identifier)
                
                writer.writerow(row)
        
        final_count = total_records - duplicates_removed
        
        print(f"   📊 Total records processed: {total_records}")
        if has_identifier and duplicates_removed > 0:
            print(f"   🔄 Removed {duplicates_removed} duplicate identifiers")
        print(f"   📊 Final record count: {final_count}")
        
        print(f"✅ Successfully created {output_file} with {final_count} records")
        
    except FileNotFoundError as e:
        print(f"❌ Error: File not found - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error processing trades files: {e}")
        sys.exit(1)

def copy_accounts_file(source_file: str, target_file: str) -> None:
    """
    Copy accounts CSV file to accounts_db.csv
    
    Args:
        source_file: Path to source accounts CSV file
        target_file: Path to target accounts_db.csv file
    """
    print(f"📄 Copying accounts file...")
    
    try:
        print(f"   - Reading {source_file}")
        
        # Simple file copy operation using Polars for consistency
        df = pl.read_csv(source_file)
        print(f"     Found {len(df)} account records")
        print(f"📊 Columns: {df.columns}")
        
        # Save as CSV with new name
        print(f"💾 Saving accounts data to {target_file}")
        df.write_csv(target_file)
        
        print(f"✅ Successfully created {target_file} with {len(df)} records")
        
    except FileNotFoundError as e:
        print(f"❌ Error: File not found - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error processing accounts file: {e}")
        sys.exit(1)

def main():
    """Main preprocessing function"""
    print("🚀 Starting data preprocessing...")
    
    # Load environment variables
    print("🔧 Loading environment variables...")
    current_dir = Path(__file__).parent
    project_root = current_dir.parent
    env_file = project_root / ".env"
    
    if env_file.exists():
        load_dotenv(env_file)
        print(f"   ✅ Loaded environment from {env_file}")
    else:
        print(f"   ⚠️  No .env file found at {env_file}")
    
    # Validate RAW_DATA_DIR environment variable (for input files)
    raw_data_dir = os.getenv("RAW_DATA_DIR")
    if not raw_data_dir:
        print("❌ Error: RAW_DATA_DIR environment variable is not set!")
        print("Please add RAW_DATA_DIR to your .env file, example:")
        print("RAW_DATA_DIR=./raw_data")
        print("RAW_DATA_DIR=/absolute/path/to/raw_data")
        sys.exit(1)
    
    # Validate DATA_DIR environment variable (for output files)
    output_data_dir = os.getenv("DATA_DIR")
    if not output_data_dir:
        print("❌ Error: DATA_DIR environment variable is not set!")
        print("Please add DATA_DIR to your .env file, example:")
        print("DATA_DIR=./data")
        print("DATA_DIR=/absolute/path/to/data")
        sys.exit(1)
    
    # Resolve RAW_DATA_DIR path relative to project root
    print(f"🔍 Resolving RAW_DATA_DIR: {raw_data_dir}")
    
    if raw_data_dir.startswith('./') or not os.path.isabs(raw_data_dir):
        # Relative path - resolve relative to project root
        if raw_data_dir.startswith('./'):
            # Remove ./ prefix
            relative_path = raw_data_dir[2:]
        else:
            relative_path = raw_data_dir
        
        raw_data_path = project_root / relative_path
        print(f"   📁 Resolved relative path: {raw_data_dir} -> {raw_data_path}")
    else:
        # Absolute path
        raw_data_path = Path(raw_data_dir)
        print(f"   📁 Using absolute path: {raw_data_path}")
    
    # Convert to absolute path for consistency
    raw_data_path = raw_data_path.resolve()
    
    if not raw_data_path.exists():
        print(f"❌ Error: RAW_DATA_DIR path does not exist: {raw_data_path}")
        print(f"   Original value: {raw_data_dir}")
        print(f"   Resolved to: {raw_data_path}")
        sys.exit(1)
    
    print(f"✅ Using RAW_DATA_DIR: {raw_data_path}")
    
    # Resolve DATA_DIR path relative to project root
    print(f"🔍 Resolving DATA_DIR: {output_data_dir}")
    
    if output_data_dir.startswith('./') or not os.path.isabs(output_data_dir):
        # Relative path - resolve relative to project root
        if output_data_dir.startswith('./'):
            # Remove ./ prefix
            relative_path = output_data_dir[2:]
        else:
            relative_path = output_data_dir
        
        output_data_path = project_root / relative_path
        print(f"   📁 Resolved relative path: {output_data_dir} -> {output_data_path}")
    else:
        # Absolute path
        output_data_path = Path(output_data_dir)
        print(f"   📁 Using absolute path: {output_data_path}")
    
    # Convert to absolute path for consistency
    output_data_path = output_data_path.resolve()
    
    # Create output directory if it doesn't exist
    if not output_data_path.exists():
        print(f"📁 Creating DATA_DIR: {output_data_path}")
        output_data_path.mkdir(parents=True, exist_ok=True)
    
    print(f"✅ Using DATA_DIR: {output_data_path}")
    
    # Input files (using RAW_DATA_DIR path)
    trades_file1 = raw_data_path / "test_task_trades.csv"
    trades_file2 = raw_data_path / "test_task_trades_short.csv"
    accounts_file = raw_data_path / "test_task_accounts.csv"
    
    # Output files (using DATA_DIR path) - CSV format
    trades_output = output_data_path / "trades_db.csv"
    accounts_output = output_data_path / "accounts_db.csv"
    
    # Check if input files exist
    print("🔍 Checking input files...")
    missing_files = []
    
    if not trades_file1.exists():
        missing_files.append(str(trades_file1))
    if not trades_file2.exists():
        missing_files.append(str(trades_file2))
    if not accounts_file.exists():
        missing_files.append(str(accounts_file))
    
    if missing_files:
        print("❌ Missing input files:")
        for file in missing_files:
            print(f"   - {file}")
        print(f"\nPlease ensure all required CSV files are in the RAW_DATA_DIR: {raw_data_path}")
        print(f"   - {trades_file1.name}")
        print(f"   - {trades_file2.name}")
        print(f"   - {accounts_file.name}")
        sys.exit(1)
    
    print("✅ All input files found")
    
    # Process trades files
    print("\n" + "="*50)
    print("PROCESSING TRADES DATA")
    print("="*50)
    join_and_organize_trades(
        str(trades_file1),
        str(trades_file2), 
        str(trades_output)
    )
    
    # Process accounts file
    print("\n" + "="*50)
    print("PROCESSING ACCOUNTS DATA")
    print("="*50)
    copy_accounts_file(
        str(accounts_file),
        str(accounts_output)
    )
    
    print("\n" + "="*50)
    print("PREPROCESSING COMPLETE")
    print("="*50)
    print(f"📁 Output files created:")
    print(f"   - {trades_output}")
    print(f"   - {accounts_output}")
    print("\n🎉 Data preprocessing completed successfully!")

if __name__ == "__main__":
    main() 