import pytest
from aws.s3_client import (
    S3Client,
    S3GetObjectError,
    S3PutObjectError
)
from botocore.exceptions import ClientError

# ==== Fixtures ====


@pytest.fixture
def boto3_s3_client_fixture(mocker):
    boto3_mock = mocker.patch('aws.s3_client.client')
    s3_mock = mocker.MagicMock()
    boto3_mock.return_value = s3_mock
    return s3_mock

@pytest.fixture
def pd_fixture(mocker):
    return mocker.patch("aws.s3_client.pd")

@pytest.fixture
def get_object_fixture(mocker):
    return mocker.patch("aws.s3_client.S3Client.get_object")


# ==== bucket_exist ====
    

def test_bucket_exist(boto3_s3_client_fixture):
    """
    test_bucket_exist validates the response when the bucket exists
    """

    # Given
    boto3_s3_client_fixture.head_bucket.return_value = {'Body': 'body'}

    # When 
    res = S3Client().bucket_exist('test')

    # Then
    assert res == True
    boto3_s3_client_fixture.head_bucket.assert_called_once_with(Bucket='test')


def test_bucket_dont_exist(boto3_s3_client_fixture):
    """
    test_bucket_dont_exist validates the response when the bucket don't exist
    """

    # Given
    boto3_s3_client_fixture.head_bucket.side_effect = ClientError(
        error_response={}, operation_name='get_object'
    )
     # When 
    res = S3Client().bucket_exist('test')

     # Then
    assert res == False

# === get_file_keys ===

def test_get_csv_file_list(boto3_s3_client_fixture):
    """
    get_csv_file_list validates that when the function is called, the expected key
    list is returned
    """

    # Given
    bucket = 'test-bucket'
    prefix = 'test-prefix'

    mock_value =  {'Contents': [{'Key': 'object1.csv'}, {'Key': 'object2.csv'}, {'Key': 'object3'}]}
    boto3_s3_client_fixture.list_objects_v2.return_value = mock_value


    # When
    res = S3Client().get_csv_file_list(bucket, prefix)

    # Then
    boto3_s3_client_fixture.list_objects_v2.assert_called_once_with(Bucket=bucket, Prefix=prefix)
    assert res == ['object1.csv', 'object2.csv']

def test_get_csv_file_list_empty(boto3_s3_client_fixture):
    """
    test_get_file_keys_error validates that when an error arises from listing file keys,
    the S3GetFileKeysError exception is raised.
    """

    # Given
    bucket = 'test-bucket'
    prefix = 'test-prefix'

    boto3_s3_client_fixture.list_objects_v2.return_value = []

    # When
    res = S3Client().get_csv_file_list(bucket, prefix)

    # Then
    assert res == []


# ==== get_object ====


def test_get_object(boto3_s3_client_fixture):
    """
    test_get_object validates the response when an object is retrieved
    """

    # Given
    bucket = 'test-bucket'
    key = 'test-key'

    # When
    res = S3Client().get_object(bucket=bucket, file_key=key)

    # Then
    assert res == boto3_s3_client_fixture.get_object.return_value

    boto3_s3_client_fixture.get_object.assert_called_once_with(Bucket=bucket, Key=key)


def test_get_object_error(boto3_s3_client_fixture):
    """
    test_get_object_error validates that when an error arises from retrieving an
    object, the S3GetObjectError exception is raised.
    """

    # Given
    bucket = 'test-bucket'
    key = 'test-key'
    boto3_s3_client_fixture.get_object.side_effect = ClientError(
        error_response={}, operation_name='get_object'
    )

    # When
    with pytest.raises(S3GetObjectError) as e:
        S3Client().get_object(bucket, key)

    # Then
    boto3_s3_client_fixture.get_object.assert_called_once_with(Bucket=bucket, Key=key)

    assert e.value.key == key
    assert e.value.bucket == bucket
    assert e.value.message == 'Failed to get object'


# ==== put_object ====

def test_put_object(boto3_s3_client_fixture):
    """
    test_put_object validates response when an object is uploaded to bucket
    """

    # Given
    bucket = 'test-bucket'
    key = 'test-key'
    body = b'{"some": "data)'

    # When
    res = S3Client().put_object(bucket, key, body)

    # Then
    assert res == boto3_s3_client_fixture.put_object.return_value

    boto3_s3_client_fixture.put_object.assert_called_once_with(Bucket=bucket, Key=key, Body=body)

def test_put_object_error(boto3_s3_client_fixture):
    """
    test_put_object_error validates that when an error arises from uploading an
    object, the S3PutObjectError exception is raised.
    """

    # Given
    bucket = 'test-bucket'
    key = 'test-key'
    body = b'{"some": "data)'
    boto3_s3_client_fixture.put_object.side_effect = ClientError(
        error_response={}, operation_name='put_object'
    )

    # When
    with pytest.raises(S3PutObjectError) as e:
        S3Client().put_object(bucket, key, body)

    # Then
    boto3_s3_client_fixture.put_object.assert_called_once_with(Bucket=bucket, Key=key, Body=body)

    assert e.value.key == key
    assert e.value.bucket == bucket
    assert e.value.message == "Failed to put object"


# ==== export_s3_to_df ====

def test_export_s3_to_df(boto3_s3_client_fixture, pd_fixture, mocker):
    """
    test_put_object validates response when an object is uploaded to bucket
    """

    # Given
    bucket = 'test-bucket'
    file_keys = ['test_key_1', 'test_key_2']

    mock_object_1 = {"Body": b'string_1'}
    mock_object_2 = {"Body": b'string_2'}

    mock_objects = [mock_object_1, mock_object_2]
    mock_dfs = ['df1', 'df2']

    boto3_s3_client_fixture.get_object.side_effect = mock_objects
    pd_fixture.read_csv.side_effect = mock_dfs
    expected_result = pd_fixture.concat.return_value


    # When
    res = S3Client().export_s3_to_df(bucket, file_keys)

    # Then
    assert res == expected_result
    boto3_s3_client_fixture.get_object.assert_has_calls(
        [mocker.call(Bucket=bucket, Key=key) for key in file_keys]
    )
    pd_fixture.read_csv.assert_has_calls(
        [mocker.call(obj['Body']) for obj in mock_objects]
    )
    pd_fixture.concat.assert_called_with(['df1', 'df2'], ignore_index=True, sort=False)




