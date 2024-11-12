import psycopg2
from psycopg2 import pool, sql
import logging
from multiprocessing import cpu_count
from contextlib import closing

def establish_connection_pool(db_config):
    try:
        connection_pool = pool.ThreadedConnectionPool(
            db_config['min_connections'],
            max(cpu_count() * 4, 10),  # Adjusting the max connections,
            host=db_config['host'],
            database=db_config['name'],
            user=db_config['user'],
            password=db_config['password'],
            application_name='table_sync'
        )
        logging.info("Connection pool established successfully.")
        return connection_pool
    except Exception as e:
        logging.error(f"Failed to establish connection pool: {e}")
        return None

def get_column_details(connection_pool, schema_name, table_name):
    with closing(connection_pool.getconn()) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """, (schema_name, table_name))
            return {row[0]: row[1] for row in cursor.fetchall()}
    connection_pool.putconn(conn)

def synchronize_columns(connection_pool, schema_name, main_table, stage_table):
    main_columns = get_column_details(connection_pool, schema_name, main_table)
    stage_columns = get_column_details(connection_pool, schema_name, stage_table)
    
    conn = connection_pool.getconn()
    try:
        with conn.cursor() as cursor:
            for column, data_type in stage_columns.items():
                if column not in main_columns:
                    cursor.execute(sql.SQL("""
                        ALTER TABLE {}.{} ADD COLUMN {} {};
                    """).format(
                        sql.Identifier(schema_name),
                        sql.Identifier(main_table),
                        sql.Identifier(column),
                        sql.SQL(data_type)
                    ))
            
            for column in main_columns.keys():
                if column not in stage_columns:
                    cursor.execute(sql.SQL("""
                        ALTER TABLE {}.{} DROP COLUMN IF EXISTS {};
                    """).format(
                        sql.Identifier(schema_name),
                        sql.Identifier(main_table),
                        sql.Identifier(column)
                    ))
            conn.commit()
    except Exception as e:
        logging.error(f"Failed to synchronize columns: {e}")
        conn.rollback()
    finally:
        connection_pool.putconn(conn)