import pytest
from handler import aggregate_impressions
import pandas as pd

# ==== Fixtures ====

@pytest.fixture
def s3_client_fixture(mocker):
    return mocker.patch("handler.S3Client")

@pytest.fixture
def df_fixture(mocker):
    return pd.read_csv('df_fixture.scv')

# ==== aggregate_impressions ====

def test_aggregate_impressions(s3_client_fixture):

    # Given
    date_partition = '2022-04-15'
    bucket_name = 'test_bucket'
    initials = 'TI'

    mock_object_keys = ['key1', 'key2']
    mock_df = pd.read_csv('tests/unit/df_fixture.csv')
    expected_df = pd.DataFrame(
        {
            "CAMPAIGN_ID": [1111.0, 1111.0, 2222.0, 2222.0, 3333.0], 
            "HOUR": [14, 15, 12, 20, 12], 
            "IMPRESSIONS_COUNT": [2, 1, 1, 1, 2]
        }
    )
    xpected_export_object_key = 'results/2022/04/15/daily_agg_20220415_TI.csv'

    s3_client_fixture.return_value.get_csv_file_list.return_value = mock_object_keys
    s3_client_fixture.return_value.export_s3_to_df.return_value = mock_df

    # When
    aggregate_impressions(date_partition, bucket_name, initials)

    # Then
    s3_instance = s3_client_fixture.return_value
    s3_instance.get_csv_file_list.assert_called_once_with(
        bucket_name, '2022/04/15')
    s3_instance.export_s3_to_df.assert_called_once_with(
        bucket_name, mock_object_keys)
    s3_instance.export_df_to_s3.assert_called_once_with(
        bucket_name, xpected_export_object_key, expected_df)
    