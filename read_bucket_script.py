# Copyright (C) 2025 EUMETSAT
#
# This program is free software: you can redistribute it and/or modify it under the terms of the
# GNU General Public License as published by the Free Software Foundation, either version 3 of the License,
# or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
# without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with this program.
# If not, see <https://www.gnu.org/licenses/>.
import fnmatch
import os

import boto3

import credentials


def download_from_bucket(prefixes, local_download_dir, file_pattern_to_download):
    # Initialize S3 client
    s3 = boto3.client(
        "s3",
        endpoint_url=credentials.S3_ENDPOINT,
        aws_access_key_id=credentials.S3_NERO_ACCESS_KEY,
        aws_secret_access_key=credentials.S3_NERO_SECRET_KEY,
    )
    if isinstance(prefixes, str):
        prefixes = [prefixes]

    for prefix in prefixes:
        if not prefix.endswith("/"):
            prefix += "/"

        print(f"Searching in {prefix}")
        # List objects under the specified prefixes
        response = s3.list_objects_v2(Bucket=credentials.S3_NERO_BUCKET_NAME, Prefix=prefix)
        # Check if there are files under the prefixes
        if "Contents" in response:
            for obj in response["Contents"]:
                file_key = obj["Key"]  # Full S3 file path
                file_name = os.path.basename(file_key)

                # Check if the file_name matches the provided file pattern
                if fnmatch.fnmatch(file_name, file_pattern_to_download):
                    local_file_path = os.path.join(local_download_dir, file_key.replace(prefix, ""))

                    # Ensure subdirectories exist locally
                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                    # Download the file
                    print(f"Downloading {file_key} to {local_file_path}")
                    s3.download_file(credentials.S3_NERO_BUCKET_NAME, file_key, local_file_path)

            print("All files matching the filepattern downloaded successfully!")
        else:
            print("No files found under", prefix)


if __name__ == "__main__":
    prefixes_to_download = "testfolder/"  # Folder prefixes (e.g., "folder/subfolder/" or a list thereof)
    file_pattern_to_download = "test*"
    target_dir = "./s3_data/"
    download_from_bucket(prefixes_to_download, target_dir, file_pattern_to_download)
