import os
import sys
from dotenv import load_dotenv
from validation import validate_input
from minio import Minio
from minio.error import S3Error
import shutil

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
        upload_files(minio_client)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

def upload_files(client):
    """Upload the entire date folder to MinIO as a zip file."""
    try:
        # Build path to date folder
        date_folder = os.path.join(DATA_FOLDER, output_date)
        
        if not os.path.exists(date_folder):
            print(f"Folder not found: {date_folder}")
            return

        # Create a zip file of the date folder
        zip_file_path = f"{date_folder}.zip"
        shutil.make_archive(date_folder, 'zip', date_folder)

        # Upload the zip file
        object_name = f"{output_date}.zip"
        client.fput_object(
            BUCKET_NAME,
            object_name,
            zip_file_path,
            content_type="application/zip"
        )
        print(f"Uploaded: {zip_file_path} to bucket: {BUCKET_NAME} as {object_name}")

        # Clean up the zip file after upload
        os.remove(zip_file_path)
    except Exception as e:
        print(f"Error uploading folder: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()