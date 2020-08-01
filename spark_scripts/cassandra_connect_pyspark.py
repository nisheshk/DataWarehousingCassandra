from    pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col, to_timestamp


def write_to_cassandra(df):
    df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="user_info", keyspace="testing")\
        .option("inferSchema",'true')\
        .option("spark.cassandra.connection.host", "172.17.0.2")\
        .option("spark.cassandra.connection.host", "172.17.0.3")\
        .option("spark.cassandra.connection.port", "9042").save()

def read_csv_file(path):
    
    df=spark.read.option("delimiter", ",").option('header','true').csv(path)
    df = df.withColumn('event_time', to_timestamp(df['event_time'], format='yyyy-MM-dd HH:mm:ss z'))
    
    return df


if __name__ == "__main__":
    spark = SparkSession.builder.appName("load-data-into_cassandra").getOrCreate()
    csv_path = "raw_data.csv"
    df = read_csv_file(csv_path)
    write_to_cassandra(df)
    
