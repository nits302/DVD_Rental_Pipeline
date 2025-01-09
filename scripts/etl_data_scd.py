import os
import sys
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from validation import validate_input
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

TABLES = [
    "actor", "address", "category", "city", "country", "customer",
    "film", "film_actor", "film_category", "inventory", "language",
    "payment", "rental", "staff", "store"
]

OUTPUT_FOLDER = "D:\DE_study\Project\dvd_rental_pipeline\data"


def connect_postgres():
    """Connect to the Postgres database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        return conn
    except Exception as e:
        print(f"Unable to connect to Postgres. Error: {e}")
        sys.exit(1)
             
def get_previous_date(current_date):
    """Lấy ngày trước đó từ current_date"""
    date_obj = datetime.strptime(current_date, '%Y%m%d')
    prev_date = date_obj - timedelta(days=1)
    return prev_date.strftime('%Y%m%d')

def read_previous_data(table_name, prev_date):
    """Đọc dữ liệu từ ngày trước đó"""
    prev_file = os.path.join(OUTPUT_FOLDER, prev_date, f"{table_name}.csv")
    if os.path.exists(prev_file):
        return pd.read_csv(prev_file)
    return None

def extract_delta_data(engine, table_name, prev_data):
    """Trích xuất dữ liệu mới hoặc được cập nhật"""
    if prev_data is None or prev_data.empty:
        # Nếu không có dữ liệu trước đó hoặc dữ liệu rỗng, lấy tất cả
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql_query(query, engine)
    
    if table_name in TABLES_WITH_LAST_UPDATE:
        # Xử lý cho các bảng có last_update
        try:
            max_last_update = prev_data['last_update'].max()
            if pd.isna(max_last_update):  # Kiểm tra giá trị NaN
                query = f"SELECT * FROM {table_name}"
            else:
                query = f"""
                SELECT * FROM {table_name}
                WHERE last_update > '{max_last_update}'
                """
        except KeyError:  # Nếu không có cột last_update
            query = f"SELECT * FROM {table_name}"
    else:
        # Xử lý cho bảng payment (dựa vào payment_date)
        try:
            max_payment_date = prev_data['payment_date'].max()
            if pd.isna(max_payment_date):  # Kiểm tra giá trị NaN
                query = f"SELECT * FROM {table_name}"
            else:
                query = f"""
                SELECT * FROM {table_name}
                WHERE payment_date > '{max_payment_date}'
                """
        except KeyError:  # Nếu không có cột payment_date
            query = f"SELECT * FROM {table_name}"
    
    try:
        new_data = pd.read_sql_query(query, engine)
        return new_data if not new_data.empty else None
    except Exception as e:
        print(f"Error executing query for {table_name}: {e}")
        return None

def main():
    try:
        output_date = sys.argv[1]
        validate_input(output_date)
        
        # Lấy ngày trước đó
        prev_date = get_previous_date(output_date)
        
        # Tạo kết nối database
        conn = connect_postgres()
        
        # Tạo thư mục output nếu chưa tồn tại
        output_dir = os.path.join(OUTPUT_FOLDER, output_date)
        os.makedirs(output_dir, exist_ok=True)
        
        for table in TABLES:
            try:
                # Đọc dữ liệu từ ngày trước
                prev_data = read_previous_data(table, prev_date)
                
                # Lấy dữ liệu delta
                delta_data = extract_delta_data(conn, table, prev_data)
                
                if delta_data is not None and not delta_data.empty:
                    # Thêm cột processed_date
                    delta_data['processed_date'] = datetime.now()
                    
                    # Lưu dữ liệu
                    output_file = os.path.join(output_dir, f"{table}.csv")
                    delta_data.to_csv(output_file, index=False)
                    print(f"Extracted delta data for {table}: {len(delta_data)} rows")
                else:
                    # Tạo file trống với header
                    output_file = os.path.join(output_dir, f"{table}.csv")
                    if prev_data is not None:
                        prev_data.head(0).to_csv(output_file, index=False)
                    print(f"No new data for {table}")
                    
            except Exception as e:
                print(f"Error processing table {table}: {e}")
                continue
                
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()