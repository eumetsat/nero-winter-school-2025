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

import datetime
import fnmatch
import os
import shutil
from datetime import datetime

import eumdac
import requests

import credentials
from get_fci_chunks_for_area import get_chunks_for_lon_lat_bbox


# This function checks if a product entry is part of the requested coverage
def get_coverage(coverage, filenames):
    chunks = []
    for pattern in coverage:
        for file in filenames:
            if fnmatch.fnmatch(file, pattern):
                chunks.append(file)
    return chunks


def filter_chunks(chunks_list, entries):
    filtered_entries = []
    chunk_patterns = [f"*_????_00{str(chunk).zfill(2)}.nc" for chunk in chunks_list]
    for entry in entries:
        for chunk_pattern in chunk_patterns:
            if fnmatch.fnmatch(entry, chunk_pattern):
                filtered_entries.append(entry)
    return filtered_entries


def main_download_fci_from_archive(start_time, end_time, collection_id, lonlat_bbox, output_folder, run_name):
    # Insert your personal key and secret into the single quotes

    # Feed the token object with your credentials
    credentials_eumdac = (credentials.EUMDAC_CONSUMER_KEY, credentials.EUMDAC_CONSUMER_SECRET)
    token = eumdac.AccessToken(credentials_eumdac)

    # Create datastore object with your token
    datastore = eumdac.DataStore(token)

    # Select an FCI collection, eg "FCI Level 1c High Resolution Image Data - MTG - 0 degree" - "EO:EUM:DAT:0665"
    selected_collection = datastore.get_collection(collection_id)

    # Retrieve datasets that match the filter
    products = selected_collection.search(
        dtstart=datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S"),
        dtend=datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S"))

    # Print found products
    print("Found products:")
    for product in products:
        print(product)

    chunks_list = get_chunks_for_lon_lat_bbox(lonlat_bbox)
    print(f"Will be retrieving chunks: {chunks_list}")

    output_folder = os.path.join(output_folder, run_name, 'fci_l1c_input_data')
    os.makedirs(output_folder, exist_ok=True)
    for product in products:
        for file in filter_chunks(chunks_list, product.entries):
            try:
                with product.open(entry=file) as fsrc, \
                        open(os.path.join(output_folder, fsrc.name), mode='wb') as fdst:
                    shutil.copyfileobj(fsrc, fdst)
                    print(f'Download of file {os.path.join(output_folder, fsrc.name)} finished.')
            except eumdac.product.ProductError as error:
                print(f"Error related to the product '{product}' while trying to download it: '{error}'")
            except requests.exceptions.ConnectionError as error:
                print(f"Error related to the connection: '{error}'")
            except requests.exceptions.RequestException as error:
                print(f"Unexpected error: {error}")


if __name__ == "__main__":
    start_time = "2024-09-14T00:00:00"
    end_time = "2024-09-20T23:59:59"
    # geographical bounds of search area
    W = -8.8
    S = 40.6
    E = -7.4
    N = 40.9
    lonlat_bbox = [W, S, E, N]

    output_folder = "/tcenas/scratch/andream/"
    run_name = "sepeumdac"

    collection_id = "EO:EUM:DAT:0665"
    main_download_fci_from_archive(start_time, end_time, collection_id, lonlat_bbox, output_folder, run_name)
    collection_id = "EO:EUM:DAT:0662"
    main_download_fci_from_archive(start_time, end_time, collection_id, lonlat_bbox, output_folder, run_name)
