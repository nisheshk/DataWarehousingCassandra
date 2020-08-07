The script 'load_data_into_cassandra.py' loads data from .csv file to Cassandra. The csv file contains 42448764 records.

To run the script, type in the following command:

```
spark-submit --conf spark.cassandra.connection.host=172.17.0.2 --conf spark.cassandra.connection.host=172.17.0.3 --packages datastax:spark-cassandra-connector:2.4.0-s_2.11 load_data_into_cassandra.py --cass_keyspace ecommerce_data --cass_table user_info --csv_file 2019-Oct.csv --incremental_run 0
```

The script 'get_user_count_per_day.py' gets the data from Cassandra table and finds the daily user count of the website.

To run the script, type in the following command:

```
spark-submit --conf spark.cassandra.connection.host=172.17.0.2 --conf spark.cassandra.connection.host=172.17.0.3 --packages datastax:spark-cassandra-connector:2.4.0-s_2.11,org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 --conf spark.mongodb.output.uri=mongodb://127.0.0.1:27017 get_user_count_per_day.py --cass_keyspace ecommerce_data --cass_table user_info --incremental_run 0 --mongo_db database_project --mongo_collection user_count_per_day
```

The script 'get_user_count_per_hour.py' gets the data from Cassandra table and finds the hourly user count of the website.

To run the script, type in the following command:

```
spark-submit --conf spark.cassandra.connection.host=172.17.0.2 --conf spark.cassandra.connection.host=172.17.0.3 --packages datastax:spark-cassandra-connector:2.4.0-s_2.11,org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 --conf spark.mongodb.output.uri=mongodb://127.0.0.1:27017 get_user_count_per_hour.py --cass_keyspace ecommerce_data --cass_table user_info --incremental_run 0 --mongo_db database_project --mongo_collection user_count_per_hour
```

<b> Please make necessary configuration changes in the above spark-submit commands. Like cassandra host, mongodb host, appropriate spark connector.

For information about the project visit https://github.com/nisheshk/DataWarehousingCassandra/blob/master/report.docx</b>
