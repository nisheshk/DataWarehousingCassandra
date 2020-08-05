"""
Date: September 24, 2020
Goal: Loads the data from csv file to cassadnra
"""

from    pyspark.sql import SparkSession
from    pyspark.sql.types import *
import  pyspark.sql.functions as F
from    pyspark.sql.functions import col, to_timestamp
import  argparse
import  sys
from    logger import logging
from    ddl_script import create_user_info_table

def write_to_cassandra(df, table, keyspace):
     """
            This method reads the data to cassandra

            Parameters:
            -----------
            df (Dataframe): The dataframe contains data from .csv file
            keyspace (string): Cassandra keyspace to which data is to be written.
            table (string): Cassandra table to which data is to be written

            Returns
            --------
    """
     
    df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table=table, keyspace=keyspace)\
        .option("inferSchema",'true')\
        .save()

def read_csv_file(path):
    """
            This method reads the csv file provided from the command line argument.

            Parameters:
            -----------
            path (string): The path of the csv file

            Returns
            --------
            df (Dataframe): The dataframe contains data from .csv file
    """
    
    df = spark.read.option("delimiter", ",").option('header','true').csv(path)

    #Conver the event_time field to timestamp
    df = df.withColumn('event_time', to_timestamp(df['event_time'], format='yyyy-MM-dd HH:mm:ss z'))

    #Derive the field 'year' and 'week' as it serves as the partition key to the cassandra table.
    df = df.withColumn('year',F.year(df.event_time))
    df = df.withColumn("week", F.date_format(F.col("event_time"), "w"))
    return df


if __name__ == "__main__":
    try:
        #Initializes logger
        logger = logging.getLogger()
        fhandler = logging.FileHandler(filename='load_data_into_cassandra.log', mode='w')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fhandler.setFormatter(formatter)
        logger.addHandler(fhandler)
        logger.setLevel(logging.INFO)
        
        parser = argparse.ArgumentParser()
        parser.add_argument("--cass_keyspace", help="keyspace")
        parser.add_argument("--cass_table", help="table")
        parser.add_argument("--incremental_run", help="Full table load or incremental run")
        parser.add_argument("--csv_file", help="input file")
        
        #Parses the arugment provided from the command line.
        args = parser.parse_args()
        if not (args.cass_keyspace and args.cass_table and args.incremental_run and args.csv_file):
            logging.error("Command line arguments are missing. Possibly --cass_keyspace --cass_table --csv_file --incremental_run ")
            sys.exit()
        if args.incremental_run not in ['0','1']:
            logging.error("Incremental run should be either 0 or 1")
            sys.exit()
            
        incremental_run = int(args.incremental_run)
        #Spawn spark session
        spark = SparkSession.builder.appName("load-data-into-cassandra").getOrCreate()

        #Run DDL script for the table
        create_user_info_table(incremental_run, args.cass_keyspace, args.cass_table, logger)

        df = read_csv_file(args.csv_file)
        write_to_cassandra(df, args.cass_table, args.cass_keyspace)
        
    except Exception as e:
        logging.error('{0}'.format(e))
        sys.exit()
