import os
import uuid
import pandas as pd
import dotenv
from databricks import sql
from databricks.sdk.core import Config

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

cfg = Config()

conn = sql.connect(
    server_hostname=cfg.host,
    http_path="/sql/1.0/warehouses/def405ae360efab4",
    credentials_provider=lambda: cfg.authenticate,
    staging_allowed_local_path=["."],

)


dotenv.load_dotenv()

client_id = os.getenv('DATABRICKS_CLIENT_ID')
client_secret = os.getenv('DATABRICKS_CLIENT_SECRET')


# ------------------------------------------------------------------------
# Sample catalog/schema/table data
# ------------------------------------------------------------------------
def get_catalogs():
    query = "show catalogs"

    with conn.cursor() as cursor:
        cursor.execute(query)
        df = cursor.fetchall_arrow().to_pandas()
    return df['catalog'].tolist()

def get_schemas(catalog: str):
    query = f"SHOW SCHEMAS IN {catalog};"

    with conn.cursor() as cursor:
        cursor.execute(query)
        df = cursor.fetchall_arrow().to_pandas()
    return df['databaseName'].tolist()

def get_tables(catalog: str, schema: str):
    query = f"SHOW tables IN {catalog}.{schema};"

    with conn.cursor() as cursor:
        cursor.execute(query)
        df = cursor.fetchall_arrow().to_pandas()
    return df['tableName'].tolist()



def get_columns(catalog: str, schema: str, table: str):
    query = f"SHOW columns IN {catalog}.{schema}.{table};"

    with conn.cursor() as cursor:
        cursor.execute(query)
        df = cursor.fetchall_arrow().to_pandas()
    return df['col_name'].tolist()




def put_file(filename: str, localfile_path: str, volume_path: str):
    """Uploads a local file into a Databricks Volume using SQL PUT with overwrite mode."""
    target_path = f"{volume_path.rstrip('/')}/{filename}"
    query = f"PUT '{localfile_path}' INTO '{target_path}' OVERWRITE"

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            print(f"Successfully uploaded {localfile_path} → {target_path}")
    except Exception as e:
        print(f"Error executing PUT command:")
        print(f"Query: {query}")
        print(f"Error type: {type(e).__name__}")
        print(f"Message: {str(e)}")


def truncate_table(table_name):
    """Truncates a table in Databricks"""
    query = f"truncate table {table_name}"

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            print(f"Successfully truncated table -> {table_name}")
    except Exception as e:
        print(f"Error executing PUT command:")
        print(f"Query: {query}")
        print(f"Error type: {type(e).__name__}")
        print(f"Message: {str(e)}")


def append_file(file_path: str, target_table: str, file_format='CSV'):
    """
    Dynamically appends data from a CSV file into an existing table while 
    aligning to the table's schema (no schema drift).
    """


    staging_view = f"tmp_staging_{uuid.uuid4().hex[:8]}"

    try:
        with conn.cursor() as cursor:
            # 1️Get target table schema
            cursor.execute(f"DESCRIBE TABLE {target_table}")
            df_schema = cursor.fetchall_arrow().to_pandas()
            df_schema = df_schema[df_schema["col_name"].notnull()]
            schema_map = dict(zip(df_schema["col_name"], df_schema["data_type"]))
            print(f"📘 Retrieved schema for {target_table}: {schema_map}")

            # Create a temp view from CSV file
            create_view_sql = f"""
                CREATE OR REPLACE TEMP VIEW {staging_view} AS
                SELECT * FROM read_files('{file_path}', format => 'csv', header => true);
            """
            cursor.execute(create_view_sql)
            print(f"✅ Created staging view {staging_view} from {file_path}")

            # Build dynamic cast SELECT
            cast_exprs = []
            for col, dtype in schema_map.items():
                cast_exprs.append(f"CAST({col} AS {dtype}) AS {col}")
            cast_sql = ", ".join(cast_exprs)

            insert_sql = f"""
                INSERT INTO {target_table}
                SELECT {cast_sql}
                FROM {staging_view};
            """

            # 4️⃣ Execute insert
            cursor.execute(insert_sql)
            print(f"✅ Data written to {target_table} with schema alignment")

    except Exception as e:
        print(f"Error appending file:")
        print(f"File path: {file_path}")
        print(f"Target table: {target_table}")
        print(f"Error type: {type(e).__name__}")
        print(f"Message: {str(e)}")

    finally:
        # Drop the temp view to clean up
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP VIEW IF EXISTS {staging_view}")
                print(f"🧹 Dropped temp view {staging_view}")
        except Exception as drop_err:
            print(f"⚠️ Warning: Could not drop temp view {staging_view}: {drop_err}")




def merge_file(file_path: str, target_table: str, merge_key: str, file_format='CSV'):
    """
    Dynamically merges (upserts) data from a CSV file into an existing Databricks table
    while aligning to the table's schema (no schema drift).
    """

    staging_view = f"tmp_merge_{uuid.uuid4().hex[:8]}"

    try:
        with conn.cursor() as cursor:
            # 1️⃣ Get target table schema
            cursor.execute(f"DESCRIBE TABLE {target_table}")
            df_schema = cursor.fetchall_arrow().to_pandas()
            df_schema = df_schema[df_schema["col_name"].notnull()]
            schema_map = dict(zip(df_schema["col_name"], df_schema["data_type"]))
            print(f"📘 Retrieved schema for {target_table}: {schema_map}")

            # 2️⃣ Create a temp view from CSV file
            create_view_sql = f"""
                CREATE OR REPLACE TEMP VIEW {staging_view} AS
                SELECT * FROM read_files('{file_path}', format => '{file_format.lower()}', header => true);
            """
            cursor.execute(create_view_sql)
            print(f"✅ Created staging view {staging_view} from {file_path}")

            # 3️⃣ Build dynamic cast SELECT
            cast_exprs = []
            for col, dtype in schema_map.items():
                cast_exprs.append(f"CAST(s.{col} AS {dtype}) AS {col}")
            cast_sql = ", ".join(cast_exprs)

            # 4️⃣ Build dynamic MERGE SQL
            merge_sql = f"""
                MERGE INTO {target_table} AS t
                USING (
                    SELECT {cast_sql}
                    FROM {staging_view} AS s
                ) AS s
                ON t.{merge_key} = s.{merge_key}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *;
            """
            print(f"🧠 Executing MERGE SQL:\n{merge_sql}")

            # 5️⃣ Execute the merge
            cursor.execute(merge_sql)
            print(f"✅ Data merged into {target_table} using key '{merge_key}'")

    except Exception as e:
        print(f"❌ Error merging file:")
        print(f"File path: {file_path}")
        print(f"Target table: {target_table}")
        print(f"Merge key: {merge_key}")
        print(f"Error type: {type(e).__name__}")
        print(f"Message: {str(e)}")

    finally:
        # 6️⃣ Drop the temp view to clean up
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP VIEW IF EXISTS {staging_view}")
                print(f"🧹 Dropped temp view {staging_view}")
        except Exception as drop_err:
            print(f"⚠️ Warning: Could not drop temp view {staging_view}: {drop_err}")
