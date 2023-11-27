import pytest
from transformations import aggregate_impressions, _is_valid_df, parse_yaml
import pandas as pd

# ==== Fixtures ====

@pytest.fixture
def is_validate_df_data_fixture(mocker):
    return mocker.patch('transformations._is_valid_df')

@pytest.fixture
def parse_yaml_fixture(mocker):
    return mocker.patch('transformations.parse_yaml')

# ==== parse_yaml ====

def test_parse_yaml():

    """
    test_parse_yaml validates yaml config is parsed to valid dictionary
    """

    # Given
    test_config_path = 'tests/unit/fixtures/config_fixture.yaml'
    expected_obj = {
        'secrets': {
            'very_secret_string': 'bla-bla-bla',
            'even_more_secret_string': 'shh-shh-shh'
        }
    }

    # When 
    res = parse_yaml(test_config_path)

    # Then
    assert res == expected_obj

# ==== _is_valid_df ====

def test__is_valid_df_true(parse_yaml_fixture):

    """
    test__is_valid_df_true checks if valid df pass validation
    """

    # Given
    test_df = pd.DataFrame({'COLUMN_1': [1,2], 'COLUMN_2': [3, None], 'COLUMN_3': [3,5]})

    parse_yaml_fixture.return_value = {
        'columns': {
            'COLUMN_1': {'nullable': False}, 
            'COLUMN_2': {'nullable': True}
            }
        }

    # When 
    res = _is_valid_df(test_df, 'some_path')

    # Then
    assert res == True

def test__is_valid_df_missing_col(parse_yaml_fixture):

    """
    test__is_valid_df_missing_col checks if df with missing col fail validation
    """

    # Given
    test_df = pd.DataFrame({'COLUMN_1': [1,2], 'COLUMN_3': [3,5]})

    parse_yaml_fixture.return_value = {
        'columns': {
            'COLUMN_1': {'nullable': False}, 
            'COLUMN_2': {'nullable': True}
            }
        }

    # When 
    res = _is_valid_df(test_df, 'some_path')

    # Then
    assert res == False

def test__is_valid_df_has_null(parse_yaml_fixture):

    """
    test__is_valid_df_has_null checks if df with not accepted null values fail validation
    """

    # Given
    test_df = pd.DataFrame({'COLUMN_1': [1, None], 'COLUMN_2': [3, None], 'COLUMN_3': [3,5]})

    parse_yaml_fixture.return_value = {
        'columns': {
            'COLUMN_1': {'nullable': False}, 
            'COLUMN_2': {'nullable': True}
            }
        }

    # When 
    res = _is_valid_df(test_df, 'some_path')

    # Then
    assert res == False


# ==== aggregate_impressions ====


def test_aggregate_impressions(is_validate_df_data_fixture):

    """
    test_aggregate_impressions validates impressions data is 
    successfully deduplicated and aggregated
    """

    # Given
    mock_df = pd.read_csv('tests/unit/fixtures/df_fixture.csv')
    expected_df = pd.DataFrame(
        {
            'CAMPAIGN_ID': [1111.0, 1111.0, 2222.0, 2222.0, 3333.0], 
            'HOUR': [14, 15, 12, 20, 12], 
            'IMPRESSIONS_COUNT': [2, 1, 1, 1, 2]
        }
    )
    schema_path = 'some_schema_path'
    is_validate_df_data_fixture.return_value = True

    # When
    res = aggregate_impressions(mock_df, schema_path)

    # Then
    assert res.equals(expected_df)

def test_aggregate_impressions_invalid_df(is_validate_df_data_fixture):

    """
    test_aggregate_impressions validates impressions data is 
    successfully deduplicated and aggregated
    """

    # Given
    mock_df = pd.read_csv('tests/unit/fixtures/df_fixture.csv')
    schema_path = 'some_schema_path'
    is_validate_df_data_fixture.return_value = False

    # When
    # Then
    with pytest.raises(ValueError, match='Impressions dataset does not match schema some_schema_path'):
        assert aggregate_impressions(mock_df, schema_path)