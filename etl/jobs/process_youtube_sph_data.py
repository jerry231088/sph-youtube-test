import logging
import os
from pyspark.sql import *
from pyspark.sql.functions import year, month, dayofmonth, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from utils.youtube import part_list
from utils.s3 import bucket_name, raw_prefix, processed_prefix

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

project_dir = "/usr/local/spark/"
jars_path = os.path.join(project_dir, "jars", "hadoop-aws-3.3.4.jar") + "," + \
            os.path.join(project_dir, "jars", "aws-java-sdk-bundle-1.12.262.jar") + "," + \
            os.path.join(project_dir, "jars", "aws-java-sdk-core-1.12.262.jar") + "," + \
            os.path.join(project_dir, "jars", "aws-java-sdk-1.12.262.jar") + "," + \
            os.path.join(project_dir, "jars", "aws-java-sdk-s3-1.12.262.jar") + "," + \
            os.path.join(project_dir, "jars", "s3-transfer-manager-2.26.31.jar") + "," + \
            os.path.join(project_dir, "jars", "hadoop-common-3.3.4.jar")


def sph_etl():
    logger.info("Initializing Spark session...")
    # # Initialize Spark session with Hadoop AWS dependency
    spark = SparkSession.builder \
        .appName("SPH youtube data process") \
        .config("spark.driver.extraJavaOptions", "-Dspark.connect=true") \
        .config("spark.hadoop.fs.s3a.access.key", "my-key") \
        .config("spark.hadoop.fs.s3a.secret.key", "my-secret") \
        .config("spark.jars", jars_path) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    logger.info("Spark session initialized successfully.")

    try:
        # spark.sparkContext.setLogLevel("DEBUG")
        # Define an empty schema
        empty_schema = StructType([
            StructField("channel_id", IntegerType(), True),
            StructField("channel_name", StringType(), True)])

        # Create an empty DataFrame with the empty schema
        merged_df = spark.createDataFrame([], empty_schema)
        play_list_df = spark.createDataFrame([], empty_schema)
        for part in part_list:
            # Read JSON files from S3
            s3_path = f"s3a://{bucket_name}/{raw_prefix}/{part}.json.gz"
            logger.info(f"Reading JSON files from S3 {s3_path}...")
            df = spark.read.option("inferSchema", True) \
                .option("compression", "gzip") \
                .json(s3_path)

            logger.info("JSON files read successfully.")

            # Show the DataFrame content
            logger.info(f"Displaying DataFrame content: {part}")
            df.show()

            logger.info(f"Displaying DataFrame schema: {part}")
            df.printSchema()

            if part != 'playList':
                if merged_df.count() == 0:
                    merged_df = df
                else:
                    merged_df = merged_df.join(df, on=["channel_name", "channel_id"], how="inner")
            else:
                play_list_df = df

        # S3 bucket path with prefix 'processed' and file name 'your_filename.parquet'
        processed_s3_path = f"s3a://{bucket_name}/{processed_prefix}"

        # S3 Object Channel Summary
        # Specify columns you want to keep
        ch_summary_columns_rename = {
            "channel_name": "channelName",
            "channel_id": "channelId"
        }
        for o, n in ch_summary_columns_rename.items():
            merged_df = merged_df.withColumnRenamed(o, n)

        ch_summary_columns = ["channelName", "channelId", "subscriberCount", "hiddenSubscriberCount", "videoCount",
                              "viewCount", "title", "customUrl", "description", "publishedAt", "country",
                              "privacyStatus", "isLinked", "madeForKids", "topicCategories"]

        # Create a new DataFrame with only the specified columns
        ch_summary_df = merged_df.select(*ch_summary_columns)
        ch_summary_df = ch_summary_df.withColumn("year", year(current_date())) \
            .withColumn("month", month(current_date())) \
            .withColumn("day", dayofmonth(current_date()))

        logger.info("Show Channel Summary dataframe...")
        ch_summary_df.show()
        ch_summary_df.printSchema()

        ch_summary_s3_path = f"{processed_s3_path}/channel_summary"

        # Reduce DataFrame to a single partition and save as a single Parquet file with Snappy compression
        logger.info("Save to S3 bucket Channel Summary dataframe...")
        ch_summary_df.write \
            .partitionBy("year", "month", "day") \
            .parquet(ch_summary_s3_path, compression="snappy")
        logger.info("Channel Summary save to S3 successful...")

        ch_playlist_columns_rename = {
            "channel_name": "channelName",
            "channel_id": "channelId",
            "etag": "eTag",
            "id": "Id",
            "snippet.publishedAt": "videoPublishedAt",
            "snippet.title": "videoTitle",
            "snippet.description": "videoDescription",
            "snippet.position": "videoPosition",
            "contentDetails.videoId": "videoId"
        }
        for o, n in ch_playlist_columns_rename.items():
            play_list_df = play_list_df.withColumnRenamed(o, n)

        # S3 Object Channel Playlist Details
        # Specify columns you want to keep
        ch_playlist_columns = ["channelName", "channelId", "playListId", "kind", "eTag", "Id", "videoPublishedAt",
                               "videoTitle", "videoDescription", "videoPosition", "videoId", "viewCount", "likeCount",
                               "favoriteCount", "commentCount"]

        # Create a new DataFrame with only the specified columns
        ch_playlist_df = play_list_df.select(*ch_playlist_columns)
        logger.info("Show Channel Playlist dataframe...")
        ch_playlist_df.show()
        ch_playlist_df.printSchema()

        ch_playlist_df = ch_playlist_df.withColumn("year", year(current_date())) \
            .withColumn("month", month(current_date())) \
            .withColumn("day", dayofmonth(current_date()))

        ch_playlist_s3_path = f"{processed_s3_path}/channel_playlist"
        # Reduce DataFrame to a single partition and save as a single Parquet file with Snappy compression
        logger.info("Save to S3 bucket Channel Playlist dataframe...")
        ch_playlist_df.write \
            .partitionBy("year", "month", "day") \
            .parquet(ch_playlist_s3_path, compression="snappy")
        logger.info("Channel Playlist save to S3 successful...")

    except Exception as ex:
        logger.error(f"Error occurred while reading CSV files or displaying DataFrame: {ex}", exc_info=True)
    finally:
        # Stop the Spark session
        logger.info("Stopping Spark session...")
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    sph_etl()
