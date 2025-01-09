import os
import sys
import snowflake.connector
from minio import Minio
import pandas as pd
from dotenv import load_dotenv
from validation import validate_input

"""Virtual Machine"""
# Load environment variables
load_dotenv()

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")

# Snowflake configuration
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

# Data path
DATA_PATH = os.getenv("DATA_OUTPUT_PATH", "/opt/airflow/data")

def get_column_definitions(csv_path):
    """Đọc file CSV và trả về định nghĩa cột cho Snowflake"""
    try:
        # Đọc CSV file
        df = pd.read_csv(csv_path)
       
        # Dictionary mapping pandas dtypes to Snowflake types
        dtype_mapping = {
            'int64': 'NUMBER',
            'float64': 'FLOAT', 
            'object': 'VARCHAR',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'category': 'VARCHAR',
            'timedelta[ns]': 'VARCHAR'
        }
       
        # Build column definitions
        columns = []
        for column, dtype in df.dtypes.items():
            # Xử lý tên cột có thể chứa ký tự đặc biệt
            column_name = f'"{column}"' if any(c in column for c in ' ,-()') else column
            snowflake_type = dtype_mapping.get(str(dtype), 'VARCHAR')
            columns.append(f"{column_name} {snowflake_type}")
           
        return ',\n    '.join(columns)
   
    except Exception as e:
        print(f"Error analyzing CSV structure: {e}")
        raise

def load_data_to_snowflake(folder_name):
    """Load data from MinIO to Snowflake"""
    try:
        # Connect to MinIO
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False
        )

        # List all objects in the folder
        objects = list(minio_client.list_objects(
            MINIO_BUCKET_NAME, 
            prefix=folder_name + "/"
        ))

        if not objects:
            print(f"No files found in folder {folder_name}.")
            # Instead of exiting, let's check local folder
            local_folder = os.path.join(DATA_PATH, folder_name)
            if not os.path.exists(local_folder):
                print(f"Local folder {local_folder} does not exist.")
                sys.exit(1)
            
            files = [f for f in os.listdir(local_folder) if f.endswith('.csv')]
            if not files:
                print(f"No CSV files found in local folder {local_folder}")
                sys.exit(1)
            
            print(f"Found {len(files)} CSV files in local folder")
            
            # Process local files
            for file_name in files:
                table_name = file_name.replace('.csv', '').upper()
                file_path = os.path.join(local_folder, file_name)
                
                # Load to Snowflake
                load_file_to_snowflake(file_path, table_name)

        else:
            print(f"Found {len(objects)} objects in MinIO")
            # Process MinIO files
            for obj in objects:
                if obj.object_name.endswith('.csv'):
                    # Download from MinIO and load to Snowflake
                    table_name = obj.object_name.split('/')[-1].replace('.csv', '').upper()
                    temp_path = f"/tmp/{obj.object_name.split('/')[-1]}"
                    
                    # Download file
                    minio_client.fget_object(
                        MINIO_BUCKET_NAME,
                        obj.object_name,
                        temp_path
                    )
                    
                    # Load to Snowflake
                    load_file_to_snowflake(temp_path, table_name)
                    
                    # Clean up temp file
                    os.remove(temp_path)

    except Exception as e:
        print(f"Error occurred: {e}")
        sys.exit(1)

def load_file_to_snowflake(file_path, table_name):
    """Load a single file to Snowflake"""
    try:
        # Create Snowflake connection
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        
        cursor = conn.cursor()
        
        # Get column definitions
        column_defs = get_column_definitions(file_path)
        
        # Create table if not exists
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {column_defs}
        )
        """
        print(f"Creating/Checking table {table_name}")
        print(create_table_sql)
        cursor.execute(create_table_sql)
        
        # Create stage
        stage_name = f"{table_name}_STAGE"
        cursor.execute(f"CREATE OR REPLACE STAGE {stage_name}")
        
        # Put file into stage
        cursor.execute(f"PUT file://{file_path} @{stage_name}")
        
        # Copy into table
        cursor.execute(f"""
        COPY INTO {table_name}
        FROM @{stage_name}
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            FIELD_DELIMITER = ','
            NULL_IF = ('NULL', 'null', '')
        )
        ON_ERROR = 'CONTINUE'
        """)
        
        print(f"Data loaded into {table_name} successfully")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error loading {table_name}: {e}")
        raise

def main():
    """Main function to orchestrate data loading process"""
    try:
        if len(sys.argv) != 2:
            print("Usage: python script.py YYYYMMDD")
            sys.exit(1)
            
        output_date = sys.argv[1]
        validate_input(output_date)
        
        print(f"Starting data load process for date: {output_date}")
        
        load_data_to_snowflake(output_date)
        
        print("Data load process completed successfully")
        
    except Exception as e:
        print(f"Error in main process: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()