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
import tarfile
from datetime import datetime, timedelta
from multiprocessing import Pool

import eumdac
import requests

from get_fci_chunks_for_area import get_chunks_for_lon_lat_bbox
from logger import setup_logger

DEFAULT_PARALLEL_DOWNLOADS = 4

logger = setup_logger(__name__)


class EumdacDownloader:
    def __init__(self, eumdac_key, eumdac_secret):
        self.datastore = self.initialise_datastore(eumdac_key, eumdac_secret)
        self.collections = {}
        self.output_folder_run = None
        return

    def initialise_datastore(self, eumdac_key, eumdac_secret):
        if eumdac_key == "YOUR_KEY":
            raise ValueError("EUMDAC credentials are invalid. "
                             f"Please edit the credentials.py file at {os.path.abspath('credentials.py')} "
                             f"and set your key and secret. Exiting.")
        # Feed the token object with your credentials
        credentials_eumdac = (eumdac_key, eumdac_secret)
        token = eumdac.AccessToken(credentials_eumdac)
        # Create a datastore object with your token
        datastore = eumdac.DataStore(token)
        logger.debug("DataStore initialized.")
        return datastore

    def search_products_for_collection(self, collection_id, start_time, end_time):
        selected_collection = self.datastore.get_collection(collection_id)

        start_time_coll = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S")
        end_time_coll = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S")

        # Retrieve datasets that match the filter
        products = selected_collection.search(
            dtstart=start_time_coll,
            dtend=end_time_coll)

        if len(products) > 0:
            # note: we go this way instead of querying selected_collection.product_type since that is super slow
            product_type = products.first().product_type
            if '2' in product_type:
                # add and remove one second to avoid retrieval of too many L2 products
                start_time_coll += timedelta(seconds=1)
                end_time_coll -= timedelta(seconds=1)

                products = selected_collection.search(
                    dtstart=start_time_coll,
                    dtend=end_time_coll)
                logger.info(
                    f"Found {len(products)} products for collection_id {collection_id} with type {product_type} and time range {start_time_coll} to {end_time_coll} (adjusted for L2).")
            else:
                logger.info(
                    f"Found {len(products)} products for collection_id {collection_id} with type {product_type} and time range {start_time_coll} to {end_time_coll}.")

            self.collections[collection_id] = {
                'products': products,
                'product_type': product_type
            }
        else:
            logger.info(
                f"No products found for collection_id {collection_id} and time range {start_time_coll} to {end_time_coll}.")

        return len(products)

    def search_products_for_collections(self, collection_ids, start_time, end_time):
        for collection_id in collection_ids:
            self.search_products_for_collection(collection_id, start_time, end_time)

    def download_products_for_collection(self, collection_id, output_folder, run_name, file_endings, lonlat_bbox=None,
                                         n_parallel_downloads=None):
        if collection_id not in self.collections:
            logger.info(f"Collection {collection_id} not found in the collections dictionary.")
            return

        self.output_folder_run = os.path.join(output_folder, run_name)
        os.makedirs(self.output_folder_run, exist_ok=True)

        download_tasks = []

        if 'FCI1C' in self.collections[collection_id]['product_type']:
            chunks_list = get_chunks_for_lon_lat_bbox(lonlat_bbox)
            if lonlat_bbox is None:
                logger.info(f"No lonlat_bbox provided, will retrieve all chunks.")
            else:
                logger.info(f"Will be retrieving chunks: {chunks_list}")

            for product in self.collections[collection_id]['products']:
                for file in filter_chunks(chunks_list, product.entries):
                    output_file = os.path.join(self.output_folder_run, file)
                    if os.path.exists(output_file):
                        logger.info(f'File {output_file} already exists, skipping download.')
                    else:
                        download_tasks.append((product, file, self.output_folder_run))
        else:
            for product in self.collections[collection_id]['products']:
                for file in product.entries:
                    if file.endswith(tuple(file_endings)):
                        output_file = os.path.join(self.output_folder_run, file)
                        if os.path.exists(output_file):
                            logger.info(f'File already exists, skipping download: {output_file}')
                        else:
                            download_tasks.append((product, file, self.output_folder_run))

        n_parallel_downloads = n_parallel_downloads if n_parallel_downloads is not None else DEFAULT_PARALLEL_DOWNLOADS
        if len(download_tasks) > 0:
            logger.info(f"Starting download of {len(download_tasks)} files for collection {collection_id}.")
            # start with older data
            download_tasks = reversed(download_tasks)
            if n_parallel_downloads > 1:
                logger.info(f"Starting {n_parallel_downloads} parallel downloads.")
                with Pool(n_parallel_downloads) as pool:
                    pool.map(download_file, download_tasks)
            else:
                logger.info(f"Starting single-threaded download.")
                for task in download_tasks:
                    download_file(task)
        else:
            logger.info(f"No files to download for collection {collection_id}.")

        return

    def download_products_for_collections(self, collection_ids, output_folder, run_name, file_endings,
                                          lonlat_bbox=None, n_parallel_downloads=None):
        for collection_id in collection_ids:
            self.download_products_for_collection(collection_id, output_folder, run_name, file_endings, lonlat_bbox,
                                                  n_parallel_downloads=n_parallel_downloads)

    def create_tarball(self, output_folder, run_name):
        if self.output_folder_run is None:
            self.output_folder_run = os.path.join(output_folder, run_name)

        tarball_path = os.path.join(self.output_folder_run, f"tarball_{run_name}.tar.gz")
        logger.info(f"Creating tarball at {tarball_path}.")
        with tarfile.open(tarball_path, "w:gz") as tar:
            tar.add(self.output_folder_run, arcname=os.path.basename(self.output_folder_run))
        logger.info(f"Created tarball at {tarball_path}")
        return tarball_path


def get_coverage(coverage, filenames):
    # This function checks if a product entry is part of the requested coverage
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


def download_file(args):
    product, file, output_folder = args
    try:
        with product.open(entry=file) as fsrc, \
                open(os.path.join(output_folder, fsrc.name), mode='wb') as fdst:
            shutil.copyfileobj(fsrc, fdst)
            logger.info(f'Download of file {os.path.join(output_folder, fsrc.name)} finished.')
    except eumdac.product.ProductError as error:
        logger.info(f"Error related to the product '{product}' while trying to download it: '{error}'")
    except requests.exceptions.ConnectionError as error:
        logger.info(f"Error related to the connection: '{error}'")
    except requests.exceptions.RequestException as error:
        logger.info(f"Unexpected error: {error}")


def download_from_eumdac(start_time, end_time, collection_ids, file_endings,
                         lonlat_bbox, output_folder, run_name, eumdac_key, eumdac_secret, create_tarball=False,
                         n_parallel_downloads=None, ):
    eumdac_downloader = EumdacDownloader(eumdac_key, eumdac_secret)
    eumdac_downloader.search_products_for_collections(collection_ids, start_time, end_time)
    eumdac_downloader.download_products_for_collections(collection_ids, output_folder, run_name, file_endings,
                                                        lonlat_bbox=lonlat_bbox,
                                                        n_parallel_downloads=n_parallel_downloads)

    if create_tarball:
        eumdac_downloader.create_tarball(output_folder, run_name)
    return


if __name__ == "__main__":
    #########

    # FCI L1c data since the (pre-)op dissemination start from 24/09/2024 on is stored under
    # FDHSI: EO:EUM:DAT:0662
    # HRFI: EO:EUM:DAT:0665

    ########
    import credentials

    eumdac_key = credentials.EUMDAC_KEY
    eumdac_secret = credentials.EUMDAC_SECRET

    # geographical bounds of search area
    lonlat_bbox = [-10.5, 31.8, 57.9, 72]
    # lonlat_bbox = None

    output_folder = "/downloaded_data/"
    run_name = ""

    start_time = "2025-06-13T14:00:00"
    end_time = "2025-06-13T14:10:00"

    collection_ids = [
        "EO:EUM:DAT:0662",  # FDHSI
        "EO:EUM:DAT:0665",  # HRFI
    ]

    file_endings = ['.nc']

    download_from_eumdac("2025-08-05T14:00:00", "2025-08-06T10:59:00", collection_ids, file_endings,
                         [1, 42, 5, 44], output_folder, run_name,
                         eumdac_key, eumdac_secret)
