#!/usr/bin/env python

import sys
import argparse
from handler import process_data
from typing import List
from datetime import datetime

import logging

# Configure the logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def main(bucket_name: str, date_partition: str, initials: str, transformation_type: str):
    # Your main processing logic goes here
    logger.info(f'Bucket name: {bucket_name}')
    logger.info(f'Date partition: {date_partition}')
    logger.info(f'Initials: {initials}')
    logger.info(f'Transformation type: {transformation_type}')

    # Example processing code
    # Process files in input_dir, filter or manipulate based on date_partition, and save results to output_file.
    # ...

    process_data(
        bucket_name=bucket_name, 
        date_partition=date_partition, 
        initials=initials, 
        transformation_type=transformation_type
        )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CLI tool for processing files based on a date partition.')
    transformation_options = ['aggregate_impressions', 'other']
    
    parser.add_argument('--bucket_name', 
                        type=str, 
                        required=True, 
                        help='Bucket name to process.')

    parser.add_argument('--date_partition', 
                        type=str, 
                        required=True, 
                        help='Date partition to filter or process the files, in YYYY-MM-DD format.')
    
    parser.add_argument('--initials', 
                        type=str, 
                        required=False, 
                        default = 'Guy_Fawkes',
                        help='Initials for output filename. If not provided default value will be used')

    parser.add_argument('--transformation_type', 
                        type=str, 
                        required=True, 
                        choices=transformation_options,
                        help=f'Transformation type to apply on data. \
                            Need to choose from available options: {transformation_options}')
    
    args, leftovers = parser.parse_known_args()
    try:
        datetime.strptime(args.date_partition, '%Y-%m-%d')
    except ValueError:
        parser.error(f'--date_partition argument has incorrect format {args.date_partition}, should be YYYY-MM-DD')

    args = parser.parse_args()

    # Call the main function with parsed arguments
    main(args.bucket_name, args.date_partition, args.initials, args.transformation_type)