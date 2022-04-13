import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
import pyspark.sql.functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']
    # Upon here,related modules imported and AWS environment variables were configurated with the code.

def create_spark_session():
    # spark session creator function
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.selectExpr("song_id", "title", "artist_id", "year", "duration").orderBy(song_id).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist").parquet(os.path.join(output_data,"songs"))

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id", "name", "location", "lattitude", "longitude").orderby("artist_id").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,"artists"))


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page=='NextSong'")

    # extract columns for users table    
    artists_table = df.selectExpr("user_id", "first_name", "last_name", "gender", "level").orderBy(user_id).dropDuplicates()
    
    # write users table to parquet files
    artists_table.write.parquet(os.path.join(output_data,"artists"))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn("timestamp",get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000))
    df = df.withColumn("datetime",get_datetime(df.ts))
    
    # extract columns to create time table as start_time, hour, day, week, month, year, weekday
    time_table = df.select("datetime").withColumn("start_time",df.ts)\
                                      .withColumn("hour",hour("datetime")
                                      .withColumn("day",dayofmonth("datetime") 
                                      .withColumn("week",weekofyear("datetime")
                                      .withColumn("month",month("datetime")
                                      .withColumn("year",year("datetime")
                                      .withColumn("weakday",dayofweek("datetime")
                                      .dropDuplicates()
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(os.path.join(output_data,"time"))

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,(df.artist== song_df.artist_name) and (df.song ==song_df.title)\
                              and (df.length ==song_df.duration),"left outer")\
                        .select(df.timestamp.alias("start_time"),df.userId.alias("user_id")\
                              ,df.level,song_df.song_id,song_df.artist_id\
                              ,df.sessionId.alias("session_id"),df.location,df.userAgent.alias("user_agent")\
                              ,month(df.timestamp).alias("month"),year(df.timestsmp).alias("year"))\
                        .orderBy("user_id").withColumn("songplay_id",F.monotonically_increasing_id()).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("month","year").parquet(os.path.join(output_data,"songplays"))


def main():
    # main() function creates spark session in order to execute\ 
    #and load related data into user defined s3 pathway as output_data \
    #using by raw data from udacity-dend cluster as input_data.
    # output_data can be stated as any pathway as "s3a://<bucket name>/". Besides, it can be remain empty and code can be run locally.
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "" 
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
