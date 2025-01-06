import os
import sys
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from validation import validate_input

# Load environment variables
load_dotenv()
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

TABLES = [
    "actor",
    "address",
    "category",
    "city",
    "country",
    "customer",
    "film",
    "film_actor",
    "film_category",
    "inventory",
    "language",
    "payment",
    "rental",
    "staff",
    "store",
]

OUTPUT_FOLDER = "D:\DE_study\Project\dvd_rental_pipeline\data"
# OUTPUT_FOLDER = "/tmp"

try:
    output_date = sys.argv[1]
    validate_input(output_date)
except Exception as e:
    print(f"Error with input validation. Error: {e}")
    sys.exit(1)


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


def extract_table(conn, table_name):
    """Extract data from a specific table."""
    query = f"SELECT * FROM {table_name};"
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        print(f"Error extracting table {table_name}. Error: {e}")
        sys.exit(1)


def transform_data(df):
    """Perform basic transformations."""
    try:
        # Example transformations
        df = df.dropna()  # Drop rows with missing values
        # for col in df.select_dtypes(include=['object']).columns:
        #     df[col] = df[col].str.strip()  # Strip whitespace from strings

        # Example: Add a column indicating data was processed
        df["processed_date"] = pd.Timestamp.now()

        return df
    except Exception as e:
        print(f"Error during data transformation. Error: {e}")
        sys.exit(1)


def save_to_csv(df, table_name):
    """Save DataFrame to CSV."""
    try:
        # Create date directory
        date_dir = f"{OUTPUT_FOLDER}/{output_date}"
        os.makedirs(date_dir, exist_ok=True)
        
        # Set output path as YYYYMMDD/table.csv
        output_path = f"{date_dir}/{table_name}.csv"
        
        # Save DataFrame
        df.to_csv(output_path, index=False)
        print(f"Data for table {table_name} saved to {output_path}")
    except Exception as e:
        print(f"Error saving table {table_name} to CSV. Error: {e}")
        sys.exit(1)


def main():
    """Main function to extract, transform, and validate data."""
    conn = connect_postgres()

    for table in TABLES:
        print(f"Processing table: {table}")
        df = extract_table(conn, table)
        df = transform_data(df)

        # Validate the output data (customize as needed)
        if df.empty:
            print(f"Warning: Table {table} has no data after transformation.")

        save_to_csv(df, table)

    conn.close()


if __name__ == "__main__":
    main()
