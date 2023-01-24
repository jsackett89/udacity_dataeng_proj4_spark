import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ['AWS_SESSION_TOKEN']=config['AWS']['AWS_SESSION_TOKEN']


def create_spark_session():
    """
    This function creates a Spark session that will be used for processing the song and log data in the functions below.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function does the following:
    1) Using the `spark` session object, grabs the raw song files from the `input_data` S3 bucket location in json format and loads it into a Spark DataFrame
    2) Further processes the data into the `songs` and `artists` tables in the Sparkify star schema
    3) Writes the tables as parquet files to our `output_data` S3 bucket location
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # define song_schema on read
    song_schema = StructType([
        StructField("num_songs", IntegerType(), True),
        StructField("artist_id", StringType(), False),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("year", IntegerType(), False)
    ])

    # read song data file and drop duplicate records
    df = spark.read.json(song_data, schema = song_schema).dropDuplicates()

    # extract columns to create songs table
    song_cols = ["song_id", "title", "artist_id", "year", "duration"]

    songs_table = df.select(song_cols)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/songs.parquet')

    # extract columns to create artists table
    artist_cols = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]

    artists_table = df.selectExpr(artist_cols)

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/artists.parquet")

    # Create temp view of Spark songs_table df to use for the songplays table in the log processing
    df.createOrReplaceTempView("song_data_df")


def process_log_data(spark, input_data, output_data):
    """
    This function does the following:
    1) Using the `spark` session object, grabs the raw log files from the `input_data` S3 bucket location in json format and loads it into a Spark DataFrame
    2) Further processes the data into the `users`, `time`, and `songplays` tables in the Sparkify star schema
    3) Writes the tables as parquet files to our `output_data` S3 bucket location
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # define log_schema on read
    log_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", IntegerType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", StringType(), True),
        StructField("sessionId", StringType(), True),
        StructField("song", StringType(), True),
        StructField("status", StringType(), True),
        StructField("ts", IntegerType(), False),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True),

    ])

    # read log data file
    df = spark.read.json(log_data, schema = log_schema).dropDuplicates()

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table and drop duplicate user_ids to create a unique list
    users_cols = ["userId AS user_id", "firstName AS first_name", "lastName AS last_name", "gender", "level"]

    users_table =df.selectExpr(user_cols).dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(output_data + "users/users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))

    # create start_time column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), TimestampType())
    df = df.withColumn("start_time", get_datetime(col("ts")))

    # add date parts to df and extract columns to create time table
    df = df.withColumn("hour", hour(col("timestamp"))
    df = df.withColumn("day", day(col("timestamp")))
    df = df.withColumn("week", weekofyear(col("timestamp")))
    df = df.withColumn("month", month(col("timestamp")))
    df = df.withColumn("year", year(col("timestamp")))
    df = df.withColumn("weekday", dayofweek(col("timestamp")))

    time_cols = ["start_time", "hour", "day", "week", "month", "year", "weekday"]

    time_table = df.select(time_cols).dropDuplicates(["start_time"])

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time/time.parquet")

    # read in song data to use for songplays table
    song_df = spark.sql("SELECT song_id, title, artist_id, artist_name FROM song_data_df")

    # extract columns from joined song and log datasets to create songplays table
    conditions = [df.song == song_df.title, df.artist == song_df.artist_name]

    songplays_cols = ["start_time", "userId as user_id", "level", "song_id", "artist_id", "sessionId as session_id", "location", "userAgent as user_agent", "year", "month"]

    songplays_table = df.join(song_df, conditions, "left").selectExpr(songplays_cols).withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays/songplays.parquet")


def main():
    spark = create_spark_session()
    input_data = config['S3']['INPUT_DATA']
    output_data = config['S3']['OUTPUT_DATA']

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
