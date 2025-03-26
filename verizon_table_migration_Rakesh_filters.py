# V9 - V8 + retry for cassandra write + filter and partitioned read from oracle + Progress logging.
# V8 - Checks for Transformed parquet files in S3, then checks raw data in s3. If not found - Reads oracle data, stages raw data to s3, reads from s3 - transforms and stages transformed data in s3, loads transformed data from s3 and writes to cassandra. Includes checkpointing while writing to cassandra.
import threading
from cassandra.cluster import Cluster, PlainTextAuthProvider, NoHostAvailable
from cassandra.policies import DCAwareRoundRobinPolicy
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import json
import time
import datetime
import os
import boto3
import functools
import argparse

# Retry decorator with exponential backoff, specifically for Cassandra write errors.
def retry(func, retries=3, delay=5, backoff=2):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        _retries = retries
        _delay = delay
        while _retries > 0:
            try:
                return func(*args, **kwargs)
            except (NoHostAvailable, Exception) as e:
                _retries -= 1
                if _retries == 0:
                    raise
                logger = logging.getLogger()
                logger.warning(f"Cassandra write error: {e}. Retrying in {_delay} seconds... ({_retries} retries remaining)")
                time.sleep(_delay)
                _delay *= backoff
        return
    return wrapper

# Setting up logging to a file with a dynamic name.
def setup_logging(table_config):
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    now = datetime.datetime.now()
    log_filename = f"log_{table_config.get('target_table','migration')}_{now.strftime('%Y_%m_%d_%H_%M_%S')}.log"
    log_filepath = os.path.join(log_dir, log_filename)
    logger = logging.getLogger(log_filename)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler(log_filepath, mode='w')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(console_handler)
    return logger

# Creating and configuring a SparkSession with Cassandra connection pooling.
def create_spark_session(config_data):
    spark = SparkSession.builder \
        .appName("OracleToCassandraMigration") \
        .config("spark.cassandra.connection.host", config_data['cassandra']['host']) \
        .config("spark.cassandra.connection.port", config_data['cassandra']['port']) \
        .config("spark.cassandra.auth.username", config_data['cassandra']['username']) \
        .config("spark.cassandra.auth.password", config_data['cassandra']['password']) \
        .config("spark.cassandra.connection.keepAliveMS", "30000") \
        .config("spark.cassandra.connection.timeoutMS", "10000") \
        .config("spark.cassandra.read.timeoutMS", "60000") \
        .config("spark.cassandra.query.retry.count", "10") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.262,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
        .config("spark.sql.extensions", "com.datastax.spark.sql.CassandraSparkSessionExtension") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", config_data['s3']['access_key']) \
        .config("spark.hadoop.fs.s3a.secret.key", config_data['s3']['secret_key']) \
        .config("spark.hadoop.fs.s3a.endpoint", config_data['s3']['endpoint']) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    return spark

# Reading configuration from a JSON file.
def read_config_file(config_file_path):
    try:
        with open(config_file_path, 'r') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logging.error(f"Error reading config file: {e}")
        raise

# Reading data from Oracle table using Spark JDBC.
def read_from_oracle(spark, table_config, source_table_name, config_data, logger):
    oracle_config = config_data['oracle']
    filters = table_config.get("filters", {}).get(source_table_name, None)
    partition_column = table_config.get("partition_column", {}).get(source_table_name, None)

    try:
        logger.info(f"Reading data from Oracle table: {source_table_name}")

        reader = spark.read.format("jdbc") \
            .option("url", f"jdbc:oracle:thin:@//{oracle_config['host']}:{oracle_config['port']}/{oracle_config['database']}") \
            .option("user", oracle_config['username']) \
            .option("password", oracle_config['password']) \
            .option("driver", "oracle.jdbc.driver.OracleDriver") \
            .option("dbtable", source_table_name) \
            .option("fetchsize", table_config.get('fetch_size', 10000))

        if partition_column:
            logger.info(f"Using partition column: {partition_column} on table : {source_table_name} for parallel reads")
            reader = reader.option("partitionColumn", partition_column) \
                           .option("lowerBound", table_config.get("partition_lower_bound", {}).get(source_table_name, "1")) \
                           .option("upperBound", table_config.get("partition_upper_bound", {}).get(source_table_name, "1000000")) \
                           .option("numPartitions", table_config.get("num_partitions", 4))

        df_oracle = reader.load()  # Load the data

        if filters:
            logger.info(f"Applying filter on table : {source_table_name} WHERE {filters}")
            df_oracle = df_oracle.where(filters)  # Apply the filter
        else:
            logger.info("No filter applied.")

        record_count = df_oracle.count()
        logger.info(f"Successfully read {record_count} records from Oracle table: {source_table_name}")
        return df_oracle

    except Exception as e:
        logger.error(f"Error reading from Oracle table {source_table_name}: {e}")
        raise

# Applying the necessary transformation logic to the DataFrames.
def apply_transformation(oracle_dfs, table_config, logger):
     try:
         table_contact_df = oracle_dfs["table_contact"].alias("table_contact")
         table_x_credit_card_df = oracle_dfs["table_x_credit_card"].alias("table_x_credit_card")
         x_payment_source_df = oracle_dfs["x_payment_source"].alias("x_payment_source")
         table_address_df = oracle_dfs["table_address"].alias("table_address")

         joined_df = table_contact_df.join(table_x_credit_card_df, table_contact_df["objid"] == table_x_credit_card_df["x_credit_card2contact"]) \
             .join(x_payment_source_df, x_payment_source_df["pymt_src2x_credit_card"] == table_x_credit_card_df["objid"]) \
             .join(table_address_df, table_x_credit_card_df["x_credit_card2address"] == table_address_df["objid"])

         logger.info("Tables joined successfully.")

         transformed_df = joined_df.selectExpr(*table_config['transformed_columns'])
         logger.info("Transformation applied successfully using selectExpr from config.")
         transformed_record_count = transformed_df.count()
         logger.info(f"Total records after applying transformations: {transformed_record_count}")
         return transformed_df
     except Exception as e:
         logger.error(f"Error applying transformation: {e}")
         raise

# Checking if Parquet files already exist in the given S3 path.
def check_s3_parquet_exists(s3_path, config_data, logger):
    s3_config = config_data['s3']
    bucket_name = s3_config['bucket']

    s3 = boto3.client('s3',
                      aws_access_key_id=s3_config['access_key'],
                      aws_secret_access_key=s3_config['secret_key'],
                      endpoint_url=f"https://{s3_config['endpoint']}"
                      )

    try:
        logger.info(f"Checking for existing Parquet files in S3: {s3_path}")
        prefix = s3_path.replace(f"s3a://{bucket_name}/", "")
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.parquet'):
                    logger.info(f"Parquet files found in S3 at {s3_path}")
                    return True
            logger.info(f"No Parquet files found in S3 at {s3_path}.")
            return False
        else:
            logger.info(f"No Parquet files found in S3 at {s3_path}.")
            return False

    except Exception as e:
        logger.error(f"Error checking S3 for Parquet files: {e}")
        raise

# Staging the raw DataFrame of a single table to S3 as Parquet files.
def stage_raw_data_to_s3(df, table_config, source_table_name, config_data, logger):
    s3_config = config_data['s3']
    s3_staging_path = f"s3a://{s3_config['bucket']}/{s3_config['staging_path_raw_data']}/{source_table_name}"
    num_partitions = table_config.get("num_partitions", 10)  # Default to 10 if not in config

    try:
        logger.info(f"Staging raw data for {source_table_name} to S3 path: {s3_staging_path} as Parquet")

        # Repartition the DataFrame before writing to S3
        df_repartitioned = df.repartition(num_partitions)
        df_repartitioned.write.mode("overwrite").parquet(s3_staging_path)

        logger.info(f"Raw data for {source_table_name} successfully staged to S3 path: {s3_staging_path}")
        return s3_staging_path
    except Exception as e:
        logger.error(f"Error staging raw data for {source_table_name} to S3: {e}")
        raise


# Staging the transformed DataFrame to S3 as Parquet files.
def stage_transformed_data_to_s3(df, table_config, config_data, logger):
    s3_config = config_data['s3']
    s3_staging_path = f"s3a://{s3_config['bucket']}/{s3_config['staging_path_transformed_data']}/{table_config['target_table']}"
    num_partitions = table_config.get("num_partitions", 10)  # Default to 10 if not in config

    try:
        logger.info(f"Staging transformed data to S3 path: {s3_staging_path} as Parquet")

        # Repartition the DataFrame before writing to S3
        df_repartitioned = df.repartition(num_partitions)
        df_repartitioned.write.mode("overwrite").parquet(s3_staging_path)

        logger.info(f"Transformed data successfully staged to S3 path: {s3_staging_path}")
        return s3_staging_path
    except Exception as e:
        logger.error(f"Error staging transformed data to S3: {e}")
        raise

# Reading to DataFrame from S3 Parquet files.
def read_from_s3_parquet(spark, staging_path, logger):
    try:
        logger.info(f"Reading data from S3 path: {staging_path}")
        df_s3 = spark.read.parquet(staging_path)
        logger.info(f"Data successfully read from S3 path: {staging_path}")
        return df_s3
    except Exception as e:
        logger.error(f"Error reading data from S3 from path: {staging_path}: {e}")
        raise

# Constructs the S3 checkpoint path used for Cassandra write.
def get_cassandra_checkpoint_path(table_config, config_data):
    s3_config = config_data['s3']
    return f"s3a://{s3_config['bucket']}/{s3_config['checkpointing_path']}/{table_config['target_table']}"

# Creates an empty checkpoint file in S3 after a successful Cassandra write.
def create_cassandra_checkpoint(checkpoint_path, config_data, logger):
    s3_config = config_data['s3']
    s3 = boto3.client('s3',
                    aws_access_key_id=s3_config['access_key'],
                    aws_secret_access_key=s3_config['secret_key'],
                    endpoint_url=f"https://{s3_config['endpoint']}")
    bucket_name, key = checkpoint_path.replace("s3a://", "").split("/", 1)
    try:
        logger.info(f"Creating Cassandra write checkpoint at: {checkpoint_path}")
        s3.put_object(Bucket=bucket_name, Key=key, Body="")
        logger.info("Cassandra write checkpoint created successfully.")
    except Exception as e:
        logger.error(f"Error creating Cassandra checkpoint: {e}")
        raise

# Checks if the Cassandra write checkpoint file exists in S3.
def check_cassandra_checkpoint(checkpoint_path, config_data, logger):
    s3_config = config_data['s3']
    s3 = boto3.client('s3',
                      aws_access_key_id=s3_config['access_key'],
                      aws_secret_access_key=s3_config['secret_key'],
                      endpoint_url=f"https://{s3_config['endpoint']}")
    bucket_name, key = checkpoint_path.replace("s3a://", "").split("/", 1)
    try:
        logger.info(f"Checking for Cassandra write checkpoint at: {checkpoint_path}")
        s3.head_object(Bucket=bucket_name, Key=key)  # Check if object exists
        logger.info("Cassandra write checkpoint exists. Skipping write.")
        return True
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.info("Cassandra write checkpoint does not exist.")
            return False  # Checkpoint file does not exist
        else:
            logger.error(f"Error checking for Cassandra checkpoint: {e}")
            raise

# Listing all Parquet files in the given S3 path.
def list_parquet_files(s3_path, config_data, logger):
    s3_config = config_data['s3']
    s3 = boto3.client('s3',
                      aws_access_key_id=s3_config['access_key'],
                      aws_secret_access_key=s3_config['secret_key'],
                      endpoint_url=f"https://{s3_config['endpoint']}")
    bucket_name, prefix = s3_path.replace("s3a://", "").split("/", 1)
    parquet_files = []

    try:
        logger.info(f"Listing Parquet files in S3 path: {s3_path}")
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.parquet'):
                        parquet_files.append(obj['Key'])
        logger.info(f"Found {len(parquet_files)} Parquet files in {s3_path}")
        return parquet_files
    except Exception as e:
        logger.error(f"Error listing Parquet files in S3: {e}")
        raise

# Writing the DataFrame to Cassandra with optimized parallelism and checkpointing. Majority of the logic lies here and in migrate_table.
@retry
def write_to_cassandra(df, table_config, spark, config_data, logger):
    cassandra_config = config_data['cassandra']
    num_cassandra_nodes = len(cassandra_config['host'].split(','))
    ideal_partitions = num_cassandra_nodes * 8

    s3_config = config_data['s3']
    transformed_s3_path = f"s3a://{s3_config['bucket']}/{s3_config['staging_path_transformed_data']}/{table_config['target_table']}"
    checkpoint_path = get_cassandra_checkpoint_path(table_config, config_data)
    bucket_name = s3_config['bucket']

    # 1. List transformed parquet files from the staging directory in S3.
    parquet_files = list_parquet_files(transformed_s3_path, config_data, logger)

    total_records = 0
    for parquet_file in parquet_files:
        # Read each parquet file to get the total record count
        try:
          df = spark.read.parquet(f"s3a://{bucket_name}/{parquet_file}")
          total_records += df.count()
        except Exception as e:
          logger.error(f"Failed to read parquet file {parquet_file} to calculate total record count: {e}")
          raise

    processed_records = 0
    for parquet_file in parquet_files:
        # 2. Construct the checkpoint file path
        checkpoint_file_name = os.path.basename(parquet_file) + ".checkpoint"
        full_checkpoint_path = f"{checkpoint_path}/{checkpoint_file_name}".replace(f"s3a://{bucket_name}/","")

        # 3. Check if the checkpoint file exists
        if check_cassandra_checkpoint(f"s3a://{bucket_name}/{full_checkpoint_path}", config_data, logger):
            logger.info(f"Checkpoint found for {parquet_file}. Skipping.")
            continue  # Skip to the next Parquet file

        # 4. If checkpoint doesn't exist, read and write this Parquet file
        logger.info(f"Processing Parquet file: {parquet_file}")
        try:
            single_parquet_df = spark.read.parquet(f"s3a://{bucket_name}/{parquet_file}")
            parquet_file_record_count = single_parquet_df.count()

            # Repartition before writing.
            df_repartitioned = single_parquet_df.repartition(ideal_partitions)

            df_repartitioned.write.format("org.apache.spark.sql.cassandra") \
                .options(table=table_config['target_table'], keyspace=cassandra_config['keyspace']) \
                .option("spark.cassandra.output.concurrent.writes", "100") \
                .option("spark.cassandra.output.batch.grouping.key", "partition")\
                .option("spark.cassandra.output.batch.size.rows", "auto") \
                .mode("append") \
                .save()

            processed_records += parquet_file_record_count
            progress_percentage = (processed_records / total_records) * 100 if total_records > 0 else 0

            logger.info(f"Processed {parquet_file_record_count} records from the parquet file {parquet_file}.")
            logger.info("==================================================================================================================================")
            logger.info("==================================================================================================================================")
            logger.info(f"Successfully processed {processed_records} records out of the total {total_records}.")
            logger.info(f"Total processed: {progress_percentage:.2f}%.")
            logger.info("==================================================================================================================================")
            logger.info("==================================================================================================================================")

            # 5. Create the checkpoint file after successful write
            create_cassandra_checkpoint(f"s3a://{bucket_name}/{full_checkpoint_path}", config_data, logger)

        except Exception as e:
            logger.error(f"Failed to write {parquet_file} to Cassandra: {e}")
            raise

    logger.info(f"Cassandra write operation complete for table {table_config['target_table']}")

# Main logic that migrates transformed data to a single cassandra table, with two-phase S3 staging and checkpoint existence checks.
def migrate_table(spark, table_config, config_data):
    logger = setup_logging(table_config)
    table_name = table_config['target_table']
    logger.info(f"Starting migration for table: {table_name}")
    start_time = time.time()
    s3_config = config_data['s3']

    try:
        # Check if transformed data already exists. If yes, loads it to cassandra skipping oracle data read. If Transformed data does not exist, will need do Phase 1 and 2.
        transformed_s3_path = f"s3a://{s3_config['bucket']}/{s3_config['staging_path_transformed_data']}/{table_name}"
        if not check_s3_parquet_exists(transformed_s3_path, config_data, logger):

            # Phase 1: RAW DATA: Checkes and Stages the raw data to S3. (Only stages again if Raw data is not found in S3)
            oracle_dfs = {}
            for source_table_name in table_config['source_tables']:
                raw_s3_path = f"s3a://{s3_config['bucket']}/{s3_config['staging_path_raw_data']}/{source_table_name}"
                if not check_s3_parquet_exists(raw_s3_path, config_data, logger):
                    df_oracle = read_from_oracle(spark, table_config, source_table_name, config_data, logger)
                    stage_raw_data_to_s3(df_oracle, table_config, source_table_name, config_data, logger)
                else:
                    logger.info(f"Raw data for {source_table_name} already exists in S3. Skipping extraction.")
                # Read raw data from S3 ONLY if transformed data doesn't exist
                oracle_dfs[source_table_name] = read_from_s3_parquet(spark, raw_s3_path, logger)

            # Phase 2: TRANSFORMED DATA: Checkes and Stages the transformed data to S3. (Only stages again if transformed data is not found in S3)
            df_transformed = apply_transformation(oracle_dfs, table_config, logger)
            stage_transformed_data_to_s3(df_transformed, table_config, config_data, logger)
        else:
            # Transformed data ALREADY EXISTS. Skip to Phase 3.
            logger.info(f"Transformed data for {table_name} already exists in S3. Skipping extraction and transformation.")

        # Phase 3: Load Transformed Data from S3 to Cassandra. Main write function to cassandra.
        write_to_cassandra(None, table_config, spark, config_data, logger)  # Pass None

        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Migration for table {table_name} completed successfully in {duration:.2f} seconds.")

    except Exception as e:
        logger.error(f"Migration for table {table_name} failed: {e}")
        raise

    finally:
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

# Main function to execute the migration framework.
def main():
    parser = argparse.ArgumentParser(description="Oracle to Cassandra Migration Framework for Verizon - POC")
    parser.add_argument("config_file", help="Path to the configuration file")
    args = parser.parse_args()

    config_file_path = args.config_file
    config_data = read_config_file(config_file_path)
    spark = create_spark_session(config_data)

    global_logger = logging.getLogger()
    if not global_logger.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        global_logger.addHandler(ch)

    for table_config in config_data['tables']:
        migrate_table(spark, table_config, config_data)

    spark.stop()
    global_logger.info("Oracle to Cassandra migration framework execution completed.")
    for handler in global_logger.handlers[:]:
        handler.close()
        global_logger.removeHandler(handler)

if __name__ == "__main__":
    main()