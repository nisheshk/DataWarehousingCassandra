from    pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import pyspark
import os

def read_from_cassandra():
    df=spark.read.format("org.apache.spark.sql.cassandra").option("spark.cassandra.connection.host", "172.17.0.2")\
              .option("spark.cassandra.connection.host", "172.17.0.3")\
              .option("spark.cassandra.connection.port", "9042").option("keyspace", "testing")\
              .option("table", "user_info")\
              .load()
    return df

def get_user_count_by_hour(df):
    df.createOrReplaceTempView('df')
    result = \
        spark.sql('''

        with cte1 AS (
        SELECT DATE(event_time) as date, HOUR(event_time) as hour,
            1 as count
        FROM df
            GROUP BY
        user_session,DATE(event_time), HOUR(event_time)
        )
        SELECT to_timestamp(CONCAT(cast(date as string),"/",cast(hour as string),":00:00"), "yyyy-MM-dd/HH:mm:ss") as timestamp,
        date, hour, SUM(count) as count FROM cte1 GROUP BY date,hour
    ''')
##    result = result.withColumn("day",result["day"].cast(StringType()))
##    result = result.groupBy("year","month").agg(
##        F.map_from_entries(\
##        F.collect_list(\
##        F.struct("day", "count"))).alias("user_count"))
##    return result
    return result

def write_to_mongo(df):
    df.write.format("mongo").mode("overwrite").option("database","database_project").option("collection", "user_count_by_hour").save()


if __name__ == "__main__":

    spark = pyspark.sql.SparkSession.builder\
    .appName('test-mongo')\
    .master('local[*]')\
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/test.coll") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/test.coll") \
    .getOrCreate()
    df = read_from_cassandra()
    total_user_per_day_df = get_user_count_by_hour(df)
    #df = spark.createDataFrame([("Bilbo Baggins",  50), ("Gandalf", 1000), ("Gandalf", 195), ("Gandalf", 178), ("Kili", 77),("Kili", 169), ("Oin", 167), ("Gloin", 158)])
    write_to_mongo(total_user_per_day_df)
