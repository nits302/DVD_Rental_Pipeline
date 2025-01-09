import os
import snowflake.connector
import boto3
import pandas as pd
from botocore.exceptions import NoCredentialsError
import sys
from validation import validate_input 
from dotenv import load_dotenv
import tempfile

"""Local"""

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
   try:
       # Connect to MinIO
       s3_client = boto3.client(
           "s3",
           endpoint_url=f"http://{MINIO_ENDPOINT}",
           aws_access_key_id=MINIO_ACCESS_KEY,
           aws_secret_access_key=MINIO_SECRET_KEY,
       )

       # List files in the folder
       objects = s3_client.list_objects_v2(
           Bucket=MINIO_BUCKET_NAME, 
           Prefix=f"{folder_name}/"
       )
       
       if "Contents" not in objects:
           print(f"No files found in folder {folder_name}.")
           sys.exit(1)

       # Connect to Snowflake
       conn = snowflake.connector.connect(
           user=SNOWFLAKE_USER,
           password=SNOWFLAKE_PASSWORD,
           account=SNOWFLAKE_ACCOUNT,
           warehouse=SNOWFLAKE_WAREHOUSE,
           database=SNOWFLAKE_DATABASE,
           schema=SNOWFLAKE_SCHEMA,
       )

       cursor = conn.cursor()
       
       # Tạo thư mục tạm thời
       with tempfile.TemporaryDirectory() as temp_dir:
           for obj in objects["Contents"]:
               file_key = obj["Key"]
               if file_key.endswith(".csv"):
                   table_name = os.path.basename(file_key).replace(".csv", "")
                   download_path = os.path.join(temp_dir, os.path.basename(file_key))
                   
                   print(f"Downloading {file_key} to {download_path}")
                   
                   # Download file từ MinIO
                   s3_client.download_file(
                       Bucket=MINIO_BUCKET_NAME,
                       Key=file_key,
                       Filename=download_path
                   )

                   # Get column definitions từ CSV
                   column_definitions = get_column_definitions(download_path)
                   
                   # Create table với schema phù hợp
                   create_table_sql = f"""
                   CREATE TABLE IF NOT EXISTS {table_name} (
                       {column_definitions}
                   );
                   """
                   print(f"Creating table {table_name} with schema:")
                   print(create_table_sql)
                   cursor.execute(create_table_sql)

                   # Tạo stage
                   stage_name = f"{table_name}_stage"
                   cursor.execute(f"CREATE OR REPLACE STAGE {stage_name}")
                   
                   # Put file vào stage
                   cursor.execute(f"PUT file://{download_path} @{stage_name}")
                   
                   # Load data vào table
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
                       ON_ERROR = 'CONTINUE';
                   """)

                   print(f"Data loaded into {table_name} successfully.")

       cursor.close()
       conn.close()

   except NoCredentialsError:
       print("MinIO credentials are not valid.")
       sys.exit(1)
   except Exception as e:
       print(f"Error occurred: {e}")
       sys.exit(1)


def main():
   """Main function to orchestrate data loading process"""
    try:
       # Validate command line argument
       if len(sys.argv) != 2:
           print("Usage: python script.py YYYYMMDD")
           sys.exit(1)
           
       # Get date argument and validate
       output_date = sys.argv[1]
       validate_input(output_date)
       
       print(f"Starting data load process for date: {output_date}")
       
       # Load data to Snowflake
       load_data_to_snowflake(output_date)
       
       print("Data load process completed successfully")
       
    except Exception as e:
        print(f"Error in main process: {e}")
        sys.exit(1)

if __name__ == "__main__":
   main()