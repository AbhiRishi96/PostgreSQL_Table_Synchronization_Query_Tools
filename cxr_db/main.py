import argparse
import yaml
import pandas as pd
import logging
from cxr_db.db_utils import establish_connection_pool
from cxr_db.sync_utils import sync_tables_incremental_with_pool
from cxr_db.query_utils import create_view, execute_select_query
from cxr_db.logging_config import setup_logging

# Function to load the YAML config file
def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

# Main function to parse arguments and execute commands
def main():
    parser = argparse.ArgumentParser(description="Synchronize PostgreSQL tables, create views, or run queries.")
    
    # Sync arguments
    parser.add_argument('--sync', action='store_true', help="Synchronize tables based on configuration.")
    
    # View creation arguments (optional since fetched from config)
    parser.add_argument('--create-view', action='store_true', help="Create view as per the config file.")
    
    # Query execution arguments (optional since fetched from config)
    parser.add_argument('--select-query', action='store_true', help="Run a SELECT query as per the config file and optionally save the result.")
    parser.add_argument('--output-csv', type=str, help="Path to save query results as CSV.")
    
    # Config argument
    parser.add_argument("config", help="Path to the configuration YAML file.")
    
    args = parser.parse_args()
    
    # Load config file
    config = load_config(args.config)
    
    # Setup logging from config
    setup_logging(config['logging'])
    
    # Establish connection pool
    connection_pool = establish_connection_pool(config['database'])
    if not connection_pool:
        logging.error("Failed to establish connection pool. Exiting.")
        return
    
    # Synchronize tables if sync flag is passed
    if args.sync:
        sync_tables_incremental_with_pool(connection_pool, config)
    
    # Create view if flag is passed, fetch details from config
    if args.create_view:
        view_name = config['view']['view_name']
        select_query = config['view']['query']
        schema = config['schema']['schema_name']
        create_view(connection_pool, schema, view_name, select_query)
    
    # Run SELECT query and optionally save to CSV, fetch details from config
    if args.select_query:
        select_query = config['query']['sql']
        # Use execute_select_query to get the results
        # results, colnames = execute_select_query(connection_pool, select_query)
        df = execute_select_query(connection_pool, select_query)
        if args.output_csv:
            df.to_csv(args.output_csv, index=False)
        else:
            print(df)
    # Release connections back to the pool
    connection_pool.closeall()

if __name__ == "__main__":
    main()