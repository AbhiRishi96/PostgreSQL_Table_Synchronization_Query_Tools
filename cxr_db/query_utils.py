import logging
import pandas as pd
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from tqdm import tqdm
from contextlib import contextmanager

@contextmanager
def get_connection(connection_pool):
    conn = connection_pool.getconn()
    try:
        yield conn
    finally:
        connection_pool.putconn(conn)

def create_view(connection_pool, schema_name, view_name, select_query):
    with get_connection(connection_pool) as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(sql.SQL("DROP VIEW IF EXISTS {}.{} CASCADE").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(view_name)
                ))
                cursor.execute(sql.SQL("CREATE VIEW {}.{} AS {}").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(view_name),
                    sql.SQL(select_query)
                ))
                conn.commit()
                logging.info(f"View {schema_name}.{view_name} created successfully.")
            except Exception as e:
                logging.error(f"Failed to create view {schema_name}.{view_name}: {e}")
                conn.rollback()
                raise

def execute_select_query(connection_pool, query):
    with get_connection(connection_pool) as conn:
        with conn.cursor() as cursor:
            try:
                cursor.execute(query)
                result = cursor.fetchall()
                colnames = [desc[0] for desc in cursor.description]
                # Convert the results to a DataFrame
                df = pd.DataFrame(result, columns=colnames)

                logging.info(f"Query executed successfully: {query}")
                return df
            except Exception as e:
                logging.error(f"Failed to execute query: {e}")
                raise

def check_connection_pool(connection_pool):
    try:
        with get_connection(connection_pool) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
        return True
    except Exception as e:
        logging.error(f"Connection pool check failed: {e}")
        return False
    
# def execute_query_to_df(connection_pool, query):
#     with connection_pool.getconn() as conn:
#         with conn.cursor() as cursor:
#             try:
#                 cursor.execute(query)
#                 columns = [desc[0] for desc in cursor.description]
#                 data = cursor.fetchall()
#                 df = pd.DataFrame(data, columns=columns)
#                 return df
#             except Exception as e:
#                 print(f"Error executing query: {e}")
#                 return None
#             finally:
#                 connection_pool.putconn(conn)

# def execute_query_to_df_optimized(connection_pool, query, batch_size=100000):
#     # Establish a connection from the pool
#     conn = connection_pool.getconn()
    
#     try:
#         with conn.cursor(cursor_factory=RealDictCursor) as count_cursor:
#             # Execute the count query to get the total number of rows
#             count_query = f"SELECT COUNT(*) FROM ({query}) AS q"
#             count_cursor.execute(count_query)
#             total_rows = count_cursor.fetchone()['count']
        
#         # Initialize a list to hold DataFrames
#         dfs = []

#         # Fetch data in batches
#         with conn.cursor(cursor_factory=RealDictCursor) as cursor:
#             cursor.execute(query)
#             with tqdm(total=total_rows, desc="Fetching data", unit="rows") as pbar:
#                 while True:
#                     batch = cursor.fetchmany(batch_size)
#                     if not batch:
#                         break
#                     df_batch = pd.DataFrame(batch)
#                     dfs.append(df_batch)
#                     pbar.update(len(batch))
        
#         # Concatenate all DataFrames into one
#         final_df = pd.concat(dfs, ignore_index=True)
#         return final_df

#     except Exception as e:
#         print(f"Error executing query: {e}")
#         return None

#     finally:
#         # Return the connection to the pool
#         connection_pool.putconn(conn)