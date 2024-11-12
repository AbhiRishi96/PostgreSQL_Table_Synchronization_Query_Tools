import psycopg2
from psycopg2 import sql, pool
from psycopg2.extras import execute_values
from tqdm import tqdm
import json
from datetime import datetime
import gc
import time
from contextlib import closing, contextmanager
import logging
from cxr_db.db_utils import get_column_details, synchronize_columns

@contextmanager
def get_connection(connection_pool):
    conn = connection_pool.getconn()
    try:
        yield conn
    finally:
        connection_pool.putconn(conn)

def json_serial(obj):
    if isinstance(obj, (datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def process_batch(connection_pool, batch, schema_name, main_table, stage_table, history_table):
    main_columns = list(get_column_details(connection_pool, schema_name, main_table).keys())
    columns = ', '.join(main_columns)

    filenames = [record[main_columns.index('filenames')] for record in batch]
    
    with closing(connection_pool.getconn()) as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("""
                SELECT {} FROM {}.{} WHERE filenames = ANY(%s)
            """).format(
                sql.SQL(columns),
                sql.Identifier(schema_name),
                sql.Identifier(main_table)
            ), (filenames,))
            existing_records = {record[main_columns.index('filenames')]: record for record in cursor.fetchall()}

            insert_list = []
            update_list = []
            history_list = []

            for stage_record in batch:
                stage_dict = dict(zip(main_columns, stage_record))
                filename = stage_dict['filenames']
                stage_timestamp = stage_dict['timestamp']

                if filename in existing_records:
                    main_record = existing_records[filename]
                    main_dict = dict(zip(main_columns, main_record))
                    main_timestamp = main_dict['timestamp']

                    if stage_timestamp > main_timestamp:
                        update_list.append((stage_dict, filename))
                        history_list.append(('UPDATE', json.dumps(main_dict, default=json_serial), json.dumps(stage_dict, default=json_serial)))
                else:
                    insert_list.append(tuple(stage_dict.values()))
                    history_list.append(('INSERT', None, json.dumps(stage_dict, default=json_serial)))

            if insert_list:
                execute_values(cursor, sql.SQL("""
                    INSERT INTO {}.{} ({}) VALUES %s
                """).format(
                    sql.Identifier(schema_name),
                    sql.Identifier(main_table),
                    sql.SQL(', ').join(map(sql.Identifier, main_columns))
                ), insert_list)

            if update_list:
                update_query = sql.SQL("""
                    UPDATE {}.{} SET {} WHERE filenames = %s
                """).format(
                    sql.Identifier(schema_name),
                    sql.Identifier(main_table),
                    sql.SQL(', ').join(sql.SQL("{} = %s").format(sql.Identifier(col)) for col in main_columns if col != 'filenames')
                )
                cursor.executemany(update_query, [(tuple(stage_dict[col] for col in main_columns if col != 'filenames') + (filename,)) for stage_dict, filename in update_list])

            cursor.execute(sql.SQL("""
                DELETE FROM {}.{} m
                WHERE m.filenames = ANY(%s)
                AND NOT EXISTS (
                    SELECT 1 FROM {}.{} s
                    WHERE s.filenames = m.filenames
                ) RETURNING {}
            """).format(
                sql.Identifier(schema_name),
                sql.Identifier(main_table),
                sql.Identifier(schema_name),
                sql.Identifier(stage_table),
                sql.SQL(columns)
            ), (filenames,))
            deleted_records = cursor.fetchall()

            for deleted_record in deleted_records:
                deleted_dict = dict(zip(main_columns, deleted_record))
                history_list.append(('DELETE', json.dumps(deleted_dict, default=json_serial), None))

            if history_list:
                execute_values(cursor, sql.SQL("""
                    INSERT INTO {}.{} (operation_type, old_data, new_data, performed_by)
                    VALUES %s
                """).format(
                    sql.Identifier(schema_name),
                    sql.Identifier(history_table)
                ), [(op_type, old_data, new_data, 'current_user') for op_type, old_data, new_data in history_list])

            conn.commit()

    connection_pool.putconn(conn)
    return len(insert_list), len(update_list), len(deleted_records)

def sync_tables_incremental_with_pool(conn_pool, config):
    schema_config = config['schema']
    sync_config = config['sync']
    total_inserts = total_updates = total_deletes = 0

    try:
        synchronize_columns(conn_pool, schema_config['schema_name'], schema_config['main_table'], schema_config['stage_table'])

        main_columns = list(get_column_details(conn_pool, schema_config['schema_name'], schema_config['main_table']).keys())
        if not main_columns:
            raise ValueError(f"No columns found in {schema_config['schema_name']}.{schema_config['main_table']}!")

        with get_connection(conn_pool) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    sql.Identifier(schema_config['schema_name']),
                    sql.Identifier(schema_config['stage_table'])
                ))
                total_records = cursor.fetchone()[0]

        with tqdm(total=total_records, desc="Syncing Records") as pbar:
            for offset in range(0, total_records, sync_config['batch_size']):
                retries = 3
                while retries > 0:
                    try:
                        with get_connection(conn_pool) as conn:
                            with conn.cursor() as cursor:
                                cursor.execute(sql.SQL("""
                                    SELECT {} 
                                    FROM {}.{} 
                                    ORDER BY filenames 
                                    LIMIT {} OFFSET %s;
                                """).format(
                                    sql.SQL(', '.join(main_columns)),
                                    sql.Identifier(schema_config['schema_name']),
                                    sql.Identifier(schema_config['stage_table']),
                                    sql.Literal(sync_config['batch_size'])
                                ), (offset,))
                                stage_records = cursor.fetchall()

                        insert_count, update_count, delete_count = process_batch(
                            conn_pool, stage_records, schema_config['schema_name'], 
                            schema_config['main_table'], schema_config['stage_table'], 
                            schema_config['history_table']
                        )
                        total_inserts += insert_count
                        total_updates += update_count
                        total_deletes += delete_count
                        break
                    except (pool.PoolError, psycopg2.OperationalError) as e:
                        retries -= 1
                        if retries == 0:
                            logging.error(f"Failed to process batch at offset {offset}: {e}")
                            raise
                        logging.warning(f"Error occurred: {e}. Retrying in 1 second...")
                        time.sleep(1)

                pbar.update(len(stage_records))
                gc.collect()

        logging.info("Incremental sync completed successfully.")
        logging.info(f"Total records processed: {total_records}")
        logging.info(f"Total inserts: {total_inserts}")
        logging.info(f"Total updates: {total_updates}")
        logging.info(f"Total deletes: {total_deletes}")

    except Exception as e:
        logging.error(f"Error during synchronization: {e}")