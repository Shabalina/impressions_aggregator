from io import BytesIO
from typing import List, Optional, Dict
from boto3 import client
from botocore.exceptions import ClientError
import pandas as pd

class S3Client():
    """
    Wrapper client class which provides custom functional interactions with AWS S3 via the
    Boto3 Client
    """

    def __init__(self, config: Dict[str, Dict[str, str]] ):
        aws_config = config['aws']
        aws_access_key_id = aws_config['access_key_id']
        aws_secret_access_key = aws_config['secret_access_key']
        self.client = client('s3', 
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            region_name=aws_config['region']
                            )
    
    def bucket_exist(self, name: str) -> bool:
        """
        bucket_exist Validates if a given bucket exists.

        :param name: Name of the bucket to validate
        :return: True when the bucket exists
                 False when the bucket (i) don't exists (ii) lack of permission or (iii) client
                 error
        """
        try:
            self.client.head_bucket(Bucket=name)
            return True
        except ClientError:
            return False
        
    def get_csv_file_list(self, bucket: str, prefix: Optional[str] = None) -> List[str]:
        """
        Retrieves the list of scv files in a bucket. Optionally, a file prefix can be used
        to filter the resulting entries.

        :param bucket: the bucket name
        :param prefix: the file prefix used to filter the resulting entries. If no prefix
            is supplied, all bucket files are returned.

        :return: a list of csv file names in given bucket and prefix.
        """
        res = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)

        return [item['Key'] for item in res['Contents'] if item['Key'].endswith('.csv')] if 'Contents' in res else []
    
    def export_s3_to_df(self, bucket: str, file_keys: List[str]) -> pd.DataFrame:
        """
        Writes S3 object to pandas dataframe. 
        If few objects provided concat them all in single dataframe.

        :param bucket: The name of the S3 bucket.
        :param file_keys: The list of full destination path for s3 objects to load.
        :return: The pandas DataFrame with written data.
        """

        df_list = []
    
        for key in file_keys:
            obj = self.get_object(bucket=bucket, file_key=key)
            df_list.append(pd.read_csv(obj['Body']))

        return pd.concat(df_list, ignore_index=True, sort=False)
    

    def export_df_to_s3(
        self,
        bucket: str,
        file_key: str,
        df: pd.DataFrame,
    ) -> None:
        """
        Writes a pandas DataFrame to a CSV file and stores it in an AWS S3 bucket.

        
        :param bucket: The name of the S3 bucket.
        :param file_key: The full destination path where the file will be saved within
                    the S3 bucket, including any subdirectories and the file name itself.
        :param df: The pandas DataFrame to be written to S3.
        :return: N/A
        """

        if df.empty:
            print('No data to upload to s3')
            return

        buffer = BytesIO()
        df.to_csv(buffer, index=False)
        self.put_object(bucket=bucket, file_key=file_key, body=buffer.getvalue())

    

    def get_object(self, bucket: str, file_key: str) -> dict:
        """
        Gets S3 file content.

        :param bucket: Bucket to get from
        :param file_key: Key of object to get
        :raises S3GetObjectError: When get_object failed
        :return: S3 object as a dictionary
        """

        try:
            return self.client.get_object(Bucket=bucket, Key=file_key)
        except ClientError as e:
            raise S3GetObjectError(bucket, file_key) from e


    def put_object(self, bucket: str, file_key: str, body: bytes) -> dict:
        """
        Put object on S3 bucket

        :param bucket: Name of the bucket to write data to
        :param file_key: file key
        :param body: data in bytes
        :raises S3PutObjectError: When put_object failed
        :return: Client response
        """
        try:
            return self.client.put_object(Bucket=bucket, Key=file_key, Body=body)
        except ClientError as e:
            raise S3PutObjectError(bucket, file_key) from e


# ==== Exceptions ====


class S3PutObjectError(Exception):
    """Raised when put object failed"""

    def __init__(self, bucket, file_key, message='Failed to put object'):
        self.bucket = bucket
        self.key = file_key
        self.message = message

        super().__init__(self.message, dict(self.__dict__))


class S3GetObjectError(Exception):
    """Raised when get object failed"""

    def __init__(self, bucket, file_key, message='Failed to get object'):
        self.bucket = bucket
        self.key = file_key
        self.message = message

        super().__init__(self.message, dict(self.__dict__))
