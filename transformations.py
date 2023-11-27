import pandas as pd
from typing import Dict, Tuple
import yaml

import coloredlogs, logging

# Configure the logging
coloredlogs.install()
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_yaml(file_path: str) -> Dict:
    """
    Parse config yaml file to dictionary

    :param file_path: path to yaml config file
    :return: dictionary

    """

    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)

    return config

def _is_valid_df(df: pd.DataFrame, schema_path: str) -> bool:
    """
    Validates dataframe with raw data against required fields before transformation
    Checks presense of required fields
    When mandatory checks if column is not null

    :param df: pandas dataframe with data to validate.
    :param schema_path: path to yaml schema file with required columns.
    :return: True when data matching schema, False when data not matching schema

    """

    schema = parse_yaml(schema_path)

    columns_to_validate = {}
    for key, value in schema.get("columns", {}).items():
        key = key.strip().replace("\n", "").replace("\ufeff", "")
        columns_to_validate[key.strip()] = value

    for col, value in columns_to_validate.items():
        if not col in df:
            logger.error(f'Warning! Required column {col} is missing in dataset')
            return False
        if not value['nullable'] and pd.isnull(df[col]).any():
            logger.error(f'Warning! Column {col} has none values but should NOT be nullable')
            return False

    return True

def aggregate_impressions(
    df: pd.DataFrame, 
    schema_path: str,
    columns_to_dedup: Tuple[str, ...] = ('IMPRESSION_ID', 'IMPRESSION_DATETIME')
) -> pd.DataFrame:
    """
    Validate data for the presense of required columns
    Deduplicates data by given columns
    Aggregates impressions data by hour and campaign id

    :param df: pandas dataframe with data to transform.
    :param schema_path: path to yaml schema file with required columns.
    :param columns_to_dedup: list of columns to deduplicate by.
    :return: pandas dataframe with transformed data

    """

    # validate dataframe
    if not _is_valid_df(df, schema_path):
        raise ValueError(f'Impressions dataset does not match schema {schema_path}')

    # group deduplicates
    df.drop_duplicates(subset=columns_to_dedup, inplace=True)

    # count impressions for each campaign id at each hour
    df['IMPRESSION_DATETIME'] = pd.to_datetime(df.IMPRESSION_DATETIME, format='%Y-%m-%d %H:%M:%S')
    df['HOUR'] = df['IMPRESSION_DATETIME'].dt.hour
    grouped_df = df.groupby(['CAMPAIGN_ID','HOUR']).size().reset_index(name='IMPRESSIONS_COUNT')
    return grouped_df

def other_transformation() -> pd.DataFrame:
    """
    Placeholder function for any other possible data transformation

    :return: pandas dataframe with transformed data (empty while no logic populated)

    """

    logger.warning('Other transformation always return empty data frame')
    return pd.DataFrame()