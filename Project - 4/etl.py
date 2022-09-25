import configparser
from datetime import datetime
import os
from glob import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import (year, month, dayofmonth, hour, weekofyear, 
                                    date_format, dayofweek, monotonically_increasing_id)


# Read dl.cfg file and set parameters to envionment variables
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Get or create SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Using spark to read json files from song_data, process and extract them 
    to songs table and artists table and write into S3 with parquet format.

    Parameters:
    """

    # get filepath to song data file
    song_data = input_data + 'song_data/**/*.json'
    
    # read song data file
    df = spark.read.json(glob(song_data, recursive=True))

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration']) \
                    .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year', 'artist_id']) \
                .parquet("{}/parquet/songs".format(output_data), mode="overwrite")

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude']) \
                        .withColumnRenamed('artist_name', 'name') \
                        .withColumnRenamed('artist_location', 'location') \
                        .withColumnRenamed('artist_latitude', 'latitude') \
                        .withColumnRenamed('artist_longitude', 'longitude') \
                        .dropDuplicates() 
    
    # write artists table to parquet files
    artists_table.write.parquet("{}/parquet/artists".format(output_data), mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Using spark to read json files from log_data, process and extract them 
    to songs table and artists table and write into S3 with parquet format.
    """

    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    events_df = df.filter(df.page == 'NextSong') \
                   .select(['ts', 'userId', 'level', 'song', 'artist',
                           'length', 'sessionId', 'location', 'userAgent'])

    # extract columns for users table    
    users_table = df.select(['userId', 'firstName', 'lastName',
                            'gender', 'level']).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet("{}/parquet/users".format(output_data), mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    events_df = events_df.withColumn('timestamp', get_timestamp(events_df.ts)) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    events_df = events_df.withColumn('datetime', get_datetime(events_df.ts)) \
                        .withColumnRenamed('datetime', 'start_time')
    
    # extract columns to create time table
    time_table = events_df.select('start_time') \
                           .withColumn('hour', hour('start_time')) \
                           .withColumn('day', dayofmonth('start_time')) \
                           .withColumn('week', weekofyear('start_time')) \
                           .withColumn('month', month('start_time')) \
                           .withColumn('year', year('start_time')) \
                           .withColumn('weekday', dayofweek('start_time')) \
                           .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
                    .parquet("{}/parquet/time".format(output_data), 'overwrite')

    # read in song data to use for songplays table
    stg_songs_df = spark.read.json(glob(input_data + 'song_data/**/*.json', recursive=True))

    # extract columns from joined song and log datasets to create songplays table 
    joined_df = events_df.join(stg_songs_df, \
                        (events_df.artist == stg_songs_df.artist_name) \
                        & (events_df.song == stg_songs_df.title) \
                        & (events_df.length == stg_songs_df.duration), \
                    how='inner')

    songplays_table = joined_df.select(['start_time', 'userId', 'level', 'song_id', \
                                    'artist_id', 'sessionId', 'artist_location', 'userAgent']) \
                                .withColumnRenamed('userId', 'user_id') \
                                .withColumnRenamed('sessionId', 'session_id') \
                                .withColumnRenamed('artist_location', 'location') \
                                .withColumnRenamed('userAgent', 'user_agent') \
                                .dropDuplicates() \
                                .withColumn('year', year('start_time')) \
                                .withColumn('month', month('start_time')) \
                                .withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year', 'month']) \
            .parquet("{}/parquet/songplays".format(output_data), mode="overwrite")


def main():
    """
    Run all processes.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-anhnnt"
#     input_data = "./data/"
#     output_data = "./data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
