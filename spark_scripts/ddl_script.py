"""
Date: September 24, 2020
Goal: Creates the schema for the cassandra table.
"""

from cassandra.cluster import Cluster


def create_user_info_table(incremental_flag, keyspace, table, logger):
    """
        This method create user_info table in Cassandra which contains the user behavior data.
        Parameters:
        -----------
        incremental_flag (int): Determines if existing table needs to be destroyed so that the data is loaded again.
        keyspace (string): Cassandra keyspace name
        table (string): Cassandra table name

        Returns
        --------
    """
    try:
        logger.info("Create table in progress if incremental run is 0")
        cluster = Cluster(['172.17.0.2'])
        session = cluster.connect()
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};")
        session.set_keyspace(keyspace)

        #If incremental_flag == 0, means full table load, hence drops schema(and hence the data) and re-creates it.      
        if not incremental_flag: #Full table load
            session.execute("DROP TABLE IF EXISTS {0};".format(table))
            session.execute("""CREATE TABLE {0}(
                                event_time timestamp,
                                event_type varchar,
                                product_id double,
                                category_id double,
                                category_code varchar,
                                brand varchar,
                                user_id double,
                                price DOUBLE,
                                user_session varchar,
                                year int,
                                week int,
                                PRIMARY KEY((year,week),event_time,user_id)
                            );""".format(table))

        cluster.shutdown()
        logger.info("Function create_user_info_table() completed successfully")

    except Exception as e:
        logger.error('Error in create_user_info_table() function: {0}'.format(e))
        raise e
