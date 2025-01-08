import os
import sys
from dotenv import load_dotenv
from validation import validate_input
from minio import Minio


# Load environment variables
load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
DATA_FOLDER = "data"  # Thư mục chứa data local

# Validate input date
try:
    output_date = sys.argv[1]
    validate_input(output_date)
except Exception as e:
    print(f"Error with input validation: {e}")
    sys.exit(1)


def main():
    """Main function to upload files to MinIO"""
    try:
        # Connect to MinIO
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )

        # Ensure bucket exists
        if not minio_client.bucket_exists(BUCKET_NAME):
            minio_client.make_bucket(BUCKET_NAME)

        # Upload files from the folder
        upload_folder(minio_client)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def upload_folder(client):
    """Upload files from the date folder to MinIO maintaining folder structure."""
    try:
        # Build path to date folder
        date_folder = os.path.join(DATA_FOLDER, output_date)
        
        if not os.path.exists(date_folder):
            print(f"Folder not found: {date_folder}")
            return

        # Walk through all files in the directory
        for root, dirs, files in os.walk(date_folder):
            for file in files:
                # Get the full path of the file
                file_path = os.path.join(root, file)
                
                # Calculate relative path from the date_folder
                rel_path = os.path.relpath(file_path, DATA_FOLDER)
                
                # Use the relative path as the object name in MinIO
                object_name = rel_path.replace('\\', '/')
                
                # Get file mime type
                content_type = "application/octet-stream"
                if file.endswith('.csv'):
                    content_type = "text/csv"
                # elif file.endswith('.json'):
                #     content_type = "application/json"
                # elif file.endswith('.txt'):
                #     content_type = "text/plain"
                
                # Upload the file
                client.fput_object(
                    BUCKET_NAME,
                    object_name,
                    file_path,
                    content_type=content_type
                )
                print(f"Uploaded: {file_path} to bucket: {BUCKET_NAME} as {object_name}")

    except Exception as e:
        print(f"Error uploading folder: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()