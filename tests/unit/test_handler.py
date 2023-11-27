import pytest
from handler import process_data, _map_transformation
from transformations import aggregate_impressions, other_transformation
import pandas as pd

# ==== Fixtures ====

@pytest.fixture
def s3_instance_fixture(mocker):
    return mocker.patch('handler.S3Client').return_value

@pytest.fixture
def aggregate_impressions_fixture(mocker):
    return mocker.patch('handler.aggregate_impressions')

@pytest.fixture
def other_transformation_fixture(mocker):
    return mocker.patch('handler.other_transformation')


# ==== process_data ====

def test_process_data(s3_instance_fixture, aggregate_impressions_fixture):

    """
    test_process_data validates 
    the handler successfully process the raw files for given date
    """

    # Given
    date_partition = '2022-04-15'
    bucket_name = 'test_bucket'
    initials = 'TI'
    transformation_type = 'aggregate_impressions'

    mock_object_keys = ['key1', 'key2']
    dummy_df = pd.DataFrame({'col1': [1,2], 'col2': [3,4]})
    
    expected_export_object_key = 'results/2022/04/15/daily_agg_20220415_TI.csv'

    s3_instance_fixture.get_csv_file_list.return_value = mock_object_keys
    s3_instance_fixture.export_s3_to_df.return_value = dummy_df
    aggregate_impressions_fixture.return_value = dummy_df

    # When
    process_data(date_partition, bucket_name, initials, transformation_type)

    # Then
    s3_instance_fixture.get_csv_file_list.assert_called_once_with(
        bucket_name, '2022/04/15')
    s3_instance_fixture.export_s3_to_df.assert_called_once_with(
        bucket_name, mock_object_keys)
    s3_instance_fixture.export_df_to_s3.assert_called_once_with(
        bucket_name, expected_export_object_key, dummy_df
    )


def test_process_data_bucket_not_exist(s3_instance_fixture):

    """
    test_process_data_bucket_not_exist 
    validates the handler raise ValueError if given bucket does not exist
    """

    # Given
    date_partition = '2022-04-15'
    bucket_name = 'test_wrong_bucket'
    initials = 'TI'
    transformation_type = 'aggregate_impressions'
    
    expected_export_object_key = 'results/2022/04/15/daily_agg_20220415_TI.csv'

    s3_instance_fixture.bucket_exist.return_value = False

    # When
    # Then
    with pytest.raises(ValueError, match='No bucket exist with name test_wrong_bucket'):
        assert process_data(date_partition, bucket_name, initials, transformation_type)
    
    s3_instance_fixture.bucket_exist.assert_called_once_with(
        bucket_name)
    s3_instance_fixture.get_csv_file_list.assert_not_called()
    s3_instance_fixture.export_s3_to_df.assert_not_called()
    s3_instance_fixture.export_df_to_s3.assert_not_called()
    

def test_process_data_no_files(s3_instance_fixture, capsys):

    """
    test_process_data_no_files 
    validates the handler raise ValueError if no file keys found for given date
    """

    # Given
    date_partition = '2022-04-15'
    bucket_name = 'test_wrong_bucket'
    initials = 'TI'
    transformation_type = 'aggregate_impressions'
    
    expected_export_object_key = 'results/2022/04/15/daily_agg_20220415_TI.csv'

    s3_instance_fixture.get_csv_file_list.return_value = []

    # When
    # Then
    with pytest.raises(ValueError, match='No files to process with prefix 2022/04/15'):
        assert process_data(date_partition, bucket_name, initials, transformation_type)

    s3_instance_fixture.get_csv_file_list.assert_called_once_with(
        bucket_name, '2022/04/15')
    s3_instance_fixture.export_s3_to_df.assert_not_called()
    s3_instance_fixture.export_df_to_s3.assert_not_called()


def test_process_data_empty_df(s3_instance_fixture, aggregate_impressions_fixture):

    """
    test_process_data_empty_df 
    validates the handler exit if transformation return empty dataframe
    """

    # Given
    date_partition = '2022-04-15'
    bucket_name = 'test_wrong_bucket'
    initials = 'TI'
    transformation_type = 'aggregate_impressions'
    
    expected_export_object_key = 'results/2022/04/15/daily_agg_20220415_TI.csv'

    s3_instance_fixture.get_csv_file_list.return_value = ['key1']
    s3_instance_fixture.export_s3_to_df.return_value
    aggregate_impressions_fixture.return_value = pd.DataFrame()

    # When
    process_data(date_partition, bucket_name, initials, transformation_type)

    # Then
    s3_instance_fixture.get_csv_file_list.assert_called_once_with(
        bucket_name, '2022/04/15')
    s3_instance_fixture.export_s3_to_df.assert_called_once_with(
        bucket_name, ['key1'])
    s3_instance_fixture.export_df_to_s3.assert_not_called()


    # ==== _map_transformation ====

def test__map_transformation_aggregate_impressions(aggregate_impressions_fixture):

    """
    test__map_transformation_aggregate_impressions 
    validates correct mapping between transformation_type and transformation process
    """

    # Given
    dummy_df = pd.DataFrame()
    transformation_type = 'aggregate_impressions'
    
    #aggregate_impressions_fixture.return_value

    # When
    _map_transformation(transformation_type, dummy_df)

    # Then
    aggregate_impressions_fixture.assert_called_once_with(
        dummy_df, schema_path='schemas/impressions.yaml'
    )

def test__map_transformation_other(other_transformation_fixture):

    """
    test__map_transformation_other 
    validates other transformation_type not mapped to any transformation process
    """

    # Given

    # When
    _map_transformation('other', pd.DataFrame())

    # Then
    other_transformation_fixture.assert_called_once()

