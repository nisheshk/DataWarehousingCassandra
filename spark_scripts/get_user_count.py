"""
Date: September 27, 2020
Goal: Extracts the data from cassandra and transforms the data to find daily and
      hourly user count which is finally loaded into MongoDB.
"""

from    pyspark.sql import SparkSession
from    pyspark.sql import functions as F
from    pyspark.sql.types import StringType
from    logger import logging
from    datetime import datetime
import  pyspark
import  os
import  sys
import  argparse
import  gc

def read_from_cassandra(incremental_run, keyspace, table):
    """
        This method reads the data from cassandra based on the incremental_run
        value.If incremental value is 1, then the data read is for the running
        week and fetches the current day data from it. Else, fetches all the
        data from cassandra.

        Parameters:
        -----------
        incremental_run (int): Determines how data is to be read.
        keyspace (string): Cassandra keyspace from which data is to be read.
        table (string): Cassandra table inside the keyspace from which data is
                        to be read.

        Returns
        --------
        df (Dataframe): The dataframe obtained after reading from Cassandra
    """

    try:
        logging.info('Read from_cassandra in progress')
        column_names = ["event_time","user_id"]
        if incremental_run:
            today = date.today()
            next_day = today + timedelta(days=1)
            today_starting_timestamp = datetime(today.year, today.month, \
                                       today.day)

            next_day_starting_timestamp = datetime(next_day.year, next_day.month,\
                                          next_day.day)

            #Set condition to fetch the current day data by pushing down the
            #predicate to reduce the number of entries retrived from the database.
            incremental_condition = \
                (F.col("year") == year) & (F.col("week") == week_num) & \
                (F.col("event_time") >= today_starting_timestamp) & \
                (F.col("event_time") < next_day_starting_timestamp)

            df=spark.read.format("org.apache.spark.sql.cassandra")\
                      .option("spark.cassandra.connection.port", "9042")\
                      .option("keyspace", keyspace)\
                      .option("table", table)\
                      .load()\
                      .select(column_names)\
                      .where(incremental_condition)
        else:
            df=spark.read.format("org.apache.spark.sql.cassandra")\
                      .option("spark.cassandra.connection.port", "9042")\
                      .option("keyspace", keyspace)\
                      .option("table", table)\
                      .load()\
                      .select(column_names)

        logging.info('Dataframe loaded successfully')
        return df

    except Exception as e:
        logging.error('Error in read_from_cassandra() function: {0}'.format(e))
        raise e

def get_user_count_by_hour(df):
    """
        This method finds out the hourly user count using the e-commerce platform.

        Parameters:
        -----------
        df (Dataframe): The dataframe obtained after reading from Cassandra

        Returns
        --------
        result (Dataframe): The dataframe with the hourly user count.
    """

    try:
        logging.info('Getting user count by hour in progress')
        df.createOrReplaceTempView('df')
        result = \
            spark.sql('''
            with grouped_user_by_hour AS (
            SELECT
                user_id, DATE(event_time) as date1, HOUR(event_time) as hour,
            FROM
                df
            GROUP BY
                user_id,DATE(event_time), HOUR(event_time)
            )
            SELECT
                date1, hour, COUNT(1)
            FROM
                grouped_user_by_hour
            GROUP BY
                date1, hour
            '''
            )

        #Cached the df as it will be used again in get_user_count_by_day() function
        #cached_df = result.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
        #result.createOrReplaceTempView('result')
        # result2 = spark.sql('''
        #     SELECT
        #         to_timestamp(CONCAT(cast(date1 as string),"/",
        #         cast(hour as string),":00:00"), "yyyy-MM-dd/HH:mm:ss") as date,
        #         YEAR(date1) as year, MONTH(date1) as month, DAY(date1) as day,
        #         hour, COUNT(*) as count
        #     FROM
        #         result
        #     GROUP BY
        #         date1,hour
        # ''')
    ##    result = result.withColumn("day",result["day"].cast(StringType()))
    ##    result = result.groupBy("year","month").agg(
    ##        F.map_from_entries(\
    ##        F.collect_list(\
    ##        F.struct("day", "count"))).alias("user_count"))
    ##    return result
        logging.info('Got User count by hour successfully')
        return result

    except Exception as e:
        logging.error('Error in get_user_count_by_hour() function: {0}'.format(e))
        raise e

def get_user_count_by_day(cached_df):
    """
        This method finds out the daily user count using the platform.

        Parameters:
        -----------
        df (Dataframe): The dataframe obtained after reading from Cassandra

        Returns
        --------
        result (Dataframe): The dataframe with the daily user count.
    """

    try:
        logging.info('Getting user count by hour in progress')
        cached_df.createOrReplaceTempView('cached_df')
        user_count_per_day_df = \
                                spark.sql('''
                                with grouped_user_by_day AS (
                                SELECT
                                    user_id, DATE(event_time) as date1
                                FROM
                                    df
                                GROUP BY
                                    user_id,DATE(event_time)
                                )
                                SELECT
                                    date1, COUNT(1)
                                FROM
                                    grouped_user_by_day
                                GROUP BY
                                    date1
                                '''
                                )
    ##    result = result.withColumn("day",result["day"].cast(StringType()))
    ##    result = result.groupBy("year","month").agg(
    ##        F.map_from_entries(\
    ##        F.collect_list(\
    ##        F.struct("day", "count"))).alias("user_count"))
    ##    return result
        logging.info('Got User count by hour successfully')
        return user_count_per_day_df

    except Exception as e:
        logging.error('Error in get_user_count_by_day() function: {0}'.format(e))
        raise e

def write_to_mongo(df, database, collection, incremental_run):
    """
        This method writes the data into MongoDB.
        If incremental value is 1, then the data is appended to the database.
        Else, overwritten.

        Parameters:
        -----------
        df (Dataframe): The dataframe to be written to MongoDB.
        database (string): The database in which we are going to write the data.
        collection (string): The collection in which we are going to write the data.
        incremental_run (int): Determines if data is overwrritten or appended
        to the collection.
    """
    try:
        logging.info('Write to MongoDB in progress')
        write_mode = "overwrite"
        if incremental_run:
            write_mode = "append"
        df.write.format("mongo").mode(write_mode).option("database",database).\
            option("collection", collection).save()
        logging.info('Write to MongoDB completed successfully')

    except Exception as e:
        logging.error('Error in write_to_mongo() function: {0}'.format(e))
        raise e


if __name__ == "__main__":
    try:
        #Initializes logger
        logger = logging.getLogger()
        fhandler = logging.FileHandler(filename='user_count_by_day.log', mode='w')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s -\
                                       %(message)s')
        fhandler.setFormatter(formatter)
        logger.addHandler(fhandler)
        logger.setLevel(logging.INFO)

        #Parses the arugment provided from the command line.
        parser = argparse.ArgumentParser()
        parser.add_argument("--cass_keyspace", help="keyspace")
        parser.add_argument("--cass_table", help="table")
        parser.add_argument("--mongo_db", help="Mongo db")
        parser.add_argument("--mongo_daily_user_count_collection", \
                                help="Mongo Daily User count collection name")
        parser.add_argument("--mongo_hourly_user_count_collection", \
                                help="Mongo Hourly user count collection name")

        parser.add_argument("--incremental_run", \
            help="Full table load or incremental run")

        args = parser.parse_args()
        if not (args.cass_keyspace and args.cass_table and args.mongo_db and
                args.mongo_hourly_user_count_collection and
                args.mongo_daily_user_count_collection and
                args.incremental_run):

            logging.error("Command line arguments are missing. Possibly \
                            --cass_keyspace --cass_table --mongo_db \
                            --mongo_collection --incremental_run ")
            sys.exit()

        if args.incremental_run not in ['0','1']:
            logging.error("Incremental run should be either 0 or 1")
            sys.exit()
        incremental_run = int(args.incremental_run)

        logging.info("Argument parsed successfully")

        #Spawn spark session
        spark = pyspark.sql.SparkSession.builder\
                    .appName('get-user-count-transformation')\
                    .master('local[*]')\
                    .getOrCreate()
        df = read_from_cassandra(incremental_run, args.cass_keyspace, \
                                 args.cass_table)

        #Persist the df as it will be used twice.
        df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)

        user_count_per_hour_df = get_user_count_by_hour(df)
        write_to_mongo(user_count_per_hour_df, args.mongo_db, \
                args.mongo_hourly_user_count_collection, incremental_run)

        del user_count_per_hour_df
        gc.collect()

        user_count_per_day_df = get_user_count_by_day(df)
        write_to_mongo(user_count_per_day_df, args.mongo_db, \
                args.mongo_daily_user_count_collection, incremental_run)
        df.unpersist()

        logging.info("Script completed successully.")
        spark.stop()


    except Exception as e:
        logging.error('{0}'.format(e))
        sys.exit()
