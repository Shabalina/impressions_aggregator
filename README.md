
# Impression aggregator #
## Overview ##

The Impression aggregator is a command-line client that enables the following pipeline:
- Connecting to the given S3 bucket and retrieving the raw impressions CSV files for the given date
- Performing required transformation of the raw dataset. At the moment only one type of transformation is available:
    Extracting impression counts for each campaign by hour from the raw impressions data 
- Uploading the transformed data back to S3 bucket with a different prefix

## Business logic overview ##

- The client assumes that the raw data files are always stored in the bucket with the YYYY/MM/DD prefixes.
As the input, it requires a bucket name and a date partition in YYYY-MM-DD format to look for relevant files.
- The client can process more than 1 file for the same date partition, merging them all into a single dataset before applying the transformation.
- If provided with a bucket that does not exist or there are no files for the provided date partition or dataset do not match simple schema validation, the client will exit with an error message
- The client designed to be expanded to support transformation methods other than aggregating impressions; see the `other_transformation` method for an example.

## Limitations ##

The implementation assumes that the data fit in the memory because it loads the data and processes them with Pandas. To scale it up, the core algorithm can be rewritten using PySpark, which uses a similar programming model to pandas but can process the data off-memory.

## Setup and Installation ##

**Prerequisites**: The following requires that you have Python installed (> `v3.8`)

1. create python virtual environment

```
python3 -m venv .venv 
```
2. activate python environment

```
source .venv/bin/activate
```
3. install python packages

```
make install
```

## Client usage ##

The main file uses the argparse to parse the parameters and can print the documentation.

usage: 

```
python main.py [-h] [--bucket_name BUCKET_NAME] [--date_partition YYYY-MM-DD] [--initials GF] [--transformation_type aggregate_impressions]

```

positional arguments:

    --bucket_name           Name of the s3 bucket where raw data files are stored
    --date_partition        The date in YYY-MM-DD format for which partition to look up data files
    --initials              The user initials to customise the name of s3 file with transformed data. 
                            Optional argument, by default it has 'Guy_Fawkes' value
    --transformation_type   Type of data transformation to perform, currently there are two choices:
                            'aggregate_impressions' or 'other'

optional arguments:

  -h, --help            show this help message and exit

## AWS S3 credentials #

To process the data, the application should be able to connect to AWS S3 bucket, which is done via access_key_id and secret_access_key. The config file with keys is not included in the repo by default and needs to be recreated locally. 
To do that, create `config.yaml` file in the root of the project with the following structure:

```
aws:
  access_key_id: [YOUR_ACCESS_KEY_HERE]
  secret_access_key: [YOUR_SECRET_ACCESS_KEY_HERE]
  region: [BUCKET_REGION]

```

## Test ##

Application is covered by unit tests, based on pytest. Command to run test:

```
make test
```