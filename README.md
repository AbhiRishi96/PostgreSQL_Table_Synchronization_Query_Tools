**PostgreSQL Table Synchronization & Query Tools**

This package provides a command-line tool to interact with PostgreSQL databases for table synchronization, creating views, running select queries, and saving results to CSV files. The package is configurable via a config.yaml file, making it easy to adjust parameters like database connections, schemas, tables, queries, and more.

Features

	•	Table Synchronization: Sync data between staging tables and main tables.
	•	Create Views: Generate database views using custom SQL queries.
	•	Run Select Queries: Execute SQL queries and export the results to CSV files.
	•	Configuration: Use a YAML configuration file for managing connection details and query definitions.

**Installation**

*Prerequisites*

	•	Python 3.x
	•	PostgreSQL driver (psycopg2)
	•	pandas for handling dataframes
	•	tqdm for progress tracking

## Steps:

### Clone repository:

```bash
git clone git@github.com:qureai/rnd_database.git
    cd cxr_db 
```
    
### Install the package and dependencies:
```bash
 pip install -r requirements.txt
```

**CLI Usage**

Once the package is installed, you can use the following commands:


1. Create a Database View

To create a database view from a SQL query, run the following command:

```bash
run_cxr_db --view-name your_view_name --schema public --query "SELECT * FROM your_table" --config config.yaml
```
    •	--view-name: (Optional) Name of the view to create. Defaults to the value in config.yaml.
    •	--schema: (Optional) The schema in which to create the view. Defaults to public or the value in config.yaml.
    •	--query: (Optional) The SQL query to use for the view. If omitted, the query from config.yaml will be used.
    •	--config: (Required) Path to the config.yaml file.

*Example:*
```bash
run_cxr_db --config config.yaml
```


2. Run a SELECT Query and Save to CSV

You can run a custom SQL SELECT query and save the result to a CSV file:

```bash
run_cxr_db --select-query --save-csv config.yaml --output-file result.csv
```
Alternatively, if you want the result as a Pandas DataFrame, use the following command:

```bash
run_cxr_db --select-query config.yaml
```

*Example:*
```bash
run_cxr_db --config config.yaml --output my_results.csv
```


3. Sync Tables

To sync data between the staging table and the main table, use the following command:
```bash
run_cxr_db --sync config.yaml
```
    •	The sync command reads from the config.yaml file and synchronizes the tables in batches as defined in the sync.batch_size parameter.

4. Customize the Configuration

To change the behavior of the script, you can edit the config.yaml file directly.

-- **Changing the Database Connection:** Update the database section with your host, database name, user, and password.

```yaml
database:
  host: 'localhost'
  name: 'my_database'
  user: 'my_user'
  password: 'my_password'
```
-- **Modifying Tables:** Update the schema section if your table names differ.

```yaml
schema:
  schema_name: 'cxr'
  main_table: 'main_table_name'
  stage_table: 'staging_table_name'
  history_table: 'history_table_name'
```

-- **Adjusting Batch Size:** Change the batch size for syncing data in the sync.batch_size parameter.

```yaml
sync:
  batch_size: 10000 #adjust as per need
```
