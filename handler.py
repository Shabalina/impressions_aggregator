import pandas as pd
import yaml

from aws.s3_client import S3Client

def parse_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def aggregate_impressions(
        date_partition: str,
        bucket_name: str,
        initials: str
) -> None:
    """
    Load s3 object for given partition date to pandas dataframe.
    Filter out duplicated records.
    Count impressions per hour for each campaign id for given date.
    Upload aggregated data back to s3.

    :param date_partition: the date partition use to process file or files.
    :param bucket_name: the s3 bucket name with files to process.
    :param initials: initials to use in result filename.

    """

    s3_client = S3Client(parse_config('config.yaml'))

    if not s3_client.bucket_exist(bucket_name):
        print(f'No bucket exist with name {bucket_name}')
        return
    
    # get filenames to process
    prefix = '/'.join(date_partition.split('-'))
    print(f'Looking for files to process in {bucket_name} bucket under {prefix} prefix')

    object_keys = s3_client.get_csv_file_list(bucket_name, prefix)
    if len(object_keys) == 0:
        print(f'No files to process with prefix {prefix}')
        return

    print(f'Files to process: {object_keys}')

    # export objects content to single dataframe
    df = s3_client.export_s3_to_df(bucket_name, object_keys)
    print('initial df', df)
    columns_to_dedup = ['IMPRESSION_ID', 'IMPRESSION_DATETIME']
    df.drop_duplicates(subset=columns_to_dedup, inplace=True)
    print('dedup df', df)

    #group impression id by hour
    ## convert IMPRESSION_DATETIME to datetime
    df['IMPRESSION_DATETIME'] = pd.to_datetime(df.IMPRESSION_DATETIME, format='%Y-%m-%d %H:%M:%S')
    ## add hour column to dataframe
    df['HOUR'] = df['IMPRESSION_DATETIME'].dt.hour
    grouped_df = df.groupby(['CAMPAIGN_ID','HOUR']).size().reset_index(name='IMPRESSIONS_COUNT')

    # save processed data to s3
    

    export_object_key = 'results/{prefix}/daily_agg_{date}_{initials}.csv'\
        .format(prefix = prefix, date = ''.join(date_partition.split('-')), initials = initials)
    
    s3_client.export_df_to_s3(bucket_name, export_object_key, grouped_df)
    print(f'Data is succesfully processed and saved in s3 with prefix {export_object_key}')