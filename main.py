import sys
import argparse
from handler import aggregate_impressions
from typing import List
from datetime import datetime

def main(bucket_name, date_partition, initials):
    # Your main processing logic goes here
    print(f"Bucket name: {bucket_name}")
    print(f"Date partition: {date_partition}")
    print(f"Initials: {initials}")

    # Example processing code
    # Process files in input_dir, filter or manipulate based on date_partition, and save results to output_file.
    # ...

    aggregate_impressions(bucket_name=bucket_name, date_partition=date_partition, initials=initials)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CLI tool for processing files based on a date partition.")
    
    parser.add_argument("--bucket_name", 
                        type=str, 
                        required=True, 
                        help="Bucket name to process.")

    parser.add_argument("--date_partition", 
                        type=str, 
                        required=True, 
                        help="Date partition to filter or process the files, in YYYY-MM-DD format.")
    
    parser.add_argument("--initials", 
                        type=str, 
                        required=False, 
                        default = 'Guy_Fawkes',
                        help="Initials for output filename. If not provided default value will be used")
    
    args, leftovers = parser.parse_known_args()
    try:
        args.date_partition is not datetime.strptime(args.date_partition, '%Y-%m-%d')
    except ValueError:
        parser.error(f'--date_partition argument has incorrect format {args.date_partition}, should be YYYY-MM-DD')

    args = parser.parse_args()

    # Call the main function with parsed arguments
    main(args.bucket_name, args.date_partition, args.initials)