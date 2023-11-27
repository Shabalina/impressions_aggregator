import pandas as pd

from aws.s3_client import S3Client
from transformations import parse_yaml, aggregate_impressions, other_transformation

import coloredlogs, logging

# Configure the logging
coloredlogs.install()
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def _map_transformation(transformation_type: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply data transformation depending on transformation type

    :param transformation_type: transformation type to map with function
    :return: pandas dataframe with transformed data

    """
    if transformation_type == 'aggregate_impressions':
        return aggregate_impressions(df, schema_path='schemas/impressions.yaml')
    elif transformation_type == 'other':
        return other_transformation()
    else:
        raise ValueError(f'Wrong transformation_type {transformation_type}')

def process_data(
        date_partition: str,
        bucket_name: str,
        initials: str,
        transformation_type: str
    ) -> None:
    """
    Load s3 object for given partition date to pandas dataframe.
    Do data transformation
    Upload processed data back to s3.

    :param date_partition: the date partition use to process file or files.
    :param bucket_name: the s3 bucket name with files to process.
    :param initials: initials to use in result filename.

    """

    # set up s3 client
    s3_client = S3Client(parse_yaml('config.yaml'))

    # check if bucket name is valid
    if not s3_client.bucket_exist(bucket_name):
        logger.error(f'No bucket exist with name {bucket_name}')
        raise ValueError(f'No bucket exist with name {bucket_name}')
    
    # get file keys for given date
    prefix = '/'.join(date_partition.split('-'))
    object_keys = s3_client.get_csv_file_list(bucket_name, prefix)
    if len(object_keys) == 0:
        logger.error(f'No files to process with prefix {prefix}')
        raise ValueError(f'No files to process with prefix {prefix}')

    logger.info(f'Files to process: {object_keys}')

    # export objects content to single dataframe
    df = s3_client.export_s3_to_df(bucket_name, object_keys)
    
    #transform data
    transformed_df = _map_transformation(transformation_type, df)
    if transformed_df.empty:
        logger.warning('No data to upload to s3')
        return

    # save transformed data to s3
    export_object_key = 'results/{prefix}/daily_agg_{date}_{initials}.csv'\
        .format(prefix = prefix, date = ''.join(date_partition.split('-')), initials = initials)
    s3_client.export_df_to_s3(bucket_name, export_object_key, transformed_df)

    logger.info(f'Data is SUCCESSFULLY processed and saved in s3 with prefix {export_object_key}')