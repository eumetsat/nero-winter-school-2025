# Copyright (C) 2025 EUMETSAT
# 
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.

import eumdac
import time
import requests
import yaml
import os

import fnmatch
import shutil
from concurrent.futures import ThreadPoolExecutor

import rasterio
from rasterio.mask import mask
from shapely.geometry import box
from pathlib import Path

MAX_QUOTA = 3

import credentials

def get_config(instrument):
    if instrument == 'OLCI':
        folder_name = 'S3_OLCI'
        collectionID = 'EO:EUM:DAT:0409' #OLCI
        chain_path = 'input_graphs/olci_subset.yaml'
        orbit_direction = ''
    elif instrument == 'SLSTR_SOLAR':
        folder_name = 'S3_SLSTR_SOLAR'
        collectionID = 'EO:EUM:DAT:0411' #SLSTR
        chain_path = 'input_graphs/slstr_solar_subset.yaml'
        orbit_direction = 'DESCENDING'
    elif instrument == 'SLSTR_THERMAL':
        folder_name = 'S3_SLSTR_THERMAL'
        collectionID = 'EO:EUM:DAT:0411' #SLSTR
        chain_path = 'input_graphs/slstr_thermal_subset.yaml'
        orbit_direction = ''
    else:
        return None

    return (folder_name, collectionID, chain_path, orbit_direction)

def get_eumdac_access_token(credentials=credentials):
    consumer_key = credentials.EUMDAC_CONSUMER_KEY
    consumer_secret = credentials.EUMDAC_CONSUMER_SECRET

    credentials = (consumer_key, consumer_secret)
    token = eumdac.AccessToken(credentials)

    try:
        print(f"\nThis token '{token}' expires {token.expiration}")
        return token
    except requests.exceptions.HTTPError as exc:
        print(f"\nError when tryng the request to the server: '{exc}'")


def get_eumdac_collection(datastore, collectionID):
    # Use collection ID
    selected_collection = datastore.get_collection(collectionID)
    try:
        print(selected_collection.title)
    except eumdac.collection.CollectionError as error:
        print(f"Error related to a collection: '{error.msg}'")
    except requests.exceptions.RequestException as error:
        print(f"Unexpected error: {error}")
    return selected_collection


def search_spatially_temporally_for_products(collection, start, end, bounding_box, orbit_direction=''):
    # space/time filter the collection for products
    products = collection.search(
        bbox=bounding_box,
        dtstart=start,
        dtend=end,
        orbitdir = orbit_direction)

    print(f'Found Datasets: {products.total_results} datasets for the given time range and geographical area')
    for product in products:
        try:
            print(product)
        except eumdac.collection.CollectionError as error:
            print(f"Error related to the collection: '{error.msg}'")
        except eumdac.product.ProductError as error:
            print(f"Error related to the product: '{error.msg}'")
        except requests.exceptions.RequestException as error:
            print(f"Unexpected error: {error}")

    return products

def read_data_tailor_chain(chain_path):
    # Read the YAML file
    with open(chain_path, 'r') as file:
        yaml_data = yaml.safe_load(file)
    
    # Create the Chain object by unpacking all YAML content
    chain = eumdac.tailor_models.Chain(**yaml_data)

    return chain

def exec_data_tailor_process(product, datatailor, chain, download_dir):
    print(f"Starting Data Tailor for product at {product}")
    # Actual customisation submission
    customisation = datatailor.new_customisation(product, chain=chain)

    # Printing customisation status
    sleep_time = 10 # seconds
    while True:
        status = customisation.status
        if "DONE" in status:
            print(f"Customisation {customisation._id} is successfully completed.")
            print(f"Downloading the output of the customisation {customisation._id}")
            cust_files = fnmatch.filter(customisation.outputs, '*')[0]
            with customisation.stream_output(cust_files) as stream, open(Path(download_dir) / stream.name, mode='wb') as fdst:
                shutil.copyfileobj(stream, fdst)
                file_path = Path(download_dir) / stream.name
            print(f"Download finished for customisation {customisation._id}.")
            print(f"Finishing Data Tailor for product at {product}")
            return file_path
            break
        elif status in ["ERROR", "FAILED", "DELETED", "KILLED", "INACTIVE"]:
            print(f"Customisation {customisation._id} was unsuccessful. Customisation log is printed.\n")
            print(customisation.logfile)
            break
        elif "QUEUED" in status:
            print(f"Customisation {customisation._id} is queued.")
        elif "RUNNING" in status:
            print(f"Customisation {customisation._id} is running.")
        time.sleep(sleep_time)


def submit_data_tailor_customisations(selected_products, datatailor, chain, download_dir, 
                                      exec_data_tailor_process = exec_data_tailor_process,
                                     MAX_QUOTA = MAX_QUOTA):

    data_tailor_quota = get_data_tailor_quota(datatailor)
    # Only if over 30% of quota is used, clear the successfully finished customisations
    if get_data_tailor_space_usage_percentage(data_tailor_quota) > 30:
        clean_done_data_tailor_customisations(datatailor)

    with ThreadPoolExecutor(max_workers=MAX_QUOTA) as executor:
        futures = [executor.submit(exec_data_tailor_process, product, datatailor, chain, download_dir) 
                   for product in selected_products]

    result_paths = []
    # Get the results as they complete
    for future in futures:
        print(future.result())
        result_paths.append(future.result())

    return result_paths


def get_data_tailor_quota(datatailor):
    try:
        return datatailor.quota
    except eumdac.datatailor.DataTailorError as error:
        print(f"Error related to the Data Tailor: '{error.msg}'")
    except requests.exceptions.RequestException as error:
        print(f"Unexpected error: {error}")

def get_data_tailor_space_usage_percentage(quota):
    return quota['data'][list(quota['data'].keys())[0]]['space_usage_percentage']

def clean_all_data_tailor_customisations(datatailor):
    # Clearing all customisations from the Data Tailor
    
    for customisation in datatailor.customisations:
        if customisation.status in ['QUEUED', 'INACTIVE', 'RUNNING']:
            customisation.kill()
            print(f'Delete {customisation.status} customisation {customisation} from {customisation.creation_time} UTC.')
            try:
                customisation.delete()
            except eumdac.datatailor.CustomisationError as error:
                print("Customisation Error:", error)
            except Exception as error:
                print("Unexpected error:", error)
        else:
            print(f'Delete completed customisation {customisation} from {customisation.creation_time} UTC.')
            try:
                customisation.delete()
            except eumdac.datatailor.CustomisationError as error:
                print("Customisation Error:", error)
            except requests.exceptions.RequestException as error:
                print("Unexpected error:", error)

def clean_done_data_tailor_customisations(datatailor):
    # Clearing all customisations from the Data Tailor
    
    for customisation in datatailor.customisations:
        if customisation.status in ['DONE']:
            print(f'Delete completed customisation {customisation} from {customisation.creation_time} UTC.')
            try:
                customisation.delete()
            except eumdac.datatailor.CustomisationError as error:
                print("Customisation Error:", error)
            except requests.exceptions.RequestException as error:
                print("Unexpected error:", error)
            

def subset_and_overwrite_geotiff(input_tiff, bounding_box):
    # Input GeoTIFF path
    temp_tiff = input_tiff.parent.absolute() / "temp_subset.tif"

    bounding_box_list = [float(coord) for coord in bounding_box.split(', ')]
    # Create a geometry for the bounding box
    bbox_geom = [box(*bounding_box_list)]

    with rasterio.open(input_tiff) as src:
        # Crop the raster using the bounding box
        out_image, out_transform = mask(src, bbox_geom, crop=True)

        # Update metadata
        out_meta = src.meta.copy()
        out_meta.update({
            "driver": "GTiff",
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform
        })

        # Write the subset to a temporary file
        with rasterio.open(temp_tiff, "w", **out_meta) as dest:
            dest.write(out_image)
            # Preserve band descriptions
            dest.descriptions = src.descriptions

    # Replace the original file
    os.replace(temp_tiff, input_tiff)

    print(f"Source GeoTIFF subset: {input_tiff}")


def main_sentinel3(instrument, start_time, end_time, W, S, E, N, output_dir, run_name):

    (folder_name, collectionID, chain_path, orbit_direction) = get_config(instrument)
    
    # Create a download directory for our downloaded products    
    download_dir = Path(output_dir) / run_name / "Satellite_Imagery" / folder_name
    os.makedirs(download_dir, exist_ok=True)

    token = get_eumdac_access_token()
    # create Data Store object
    datastore = eumdac.DataStore(token)
    # get Data Store Collection
    collection = get_eumdac_collection(datastore, collectionID)

    bounding_box = f'{W}, {S}, {E}, {N}'  # West, South, East, North
    # search for product granules containing a bounding box in given timeframe
    selected_products = search_spatially_temporally_for_products(collection, start_time, end_time, bounding_box, orbit_direction)

    datatailor = eumdac.DataTailor(token)
    chain = read_data_tailor_chain(chain_path)
    
    data_tailor_quota = get_data_tailor_quota(datatailor)
    # Only if over 50% of quota is used, clear the customisations
    if get_data_tailor_space_usage_percentage(data_tailor_quota) > 50:
        clean_all_data_tailor_customisations(datatailor)

    #submit the customisations and download the data
    result_paths = submit_data_tailor_customisations(selected_products, datatailor, chain, download_dir)
    #Crop the data
    for input_tiff in result_paths:
        if input_tiff is not None:
            subset_and_overwrite_geotiff(input_tiff, bounding_box)

    
    return


if __name__ == "__main__":
    # Set sensing start and end time
    start_time = "2024-09-14T00:00:00"
    end_time = "2024-09-20T23:59:59"
    
    #  lat-lon geographical bounds of search area
    W = -9.3
    S = 40
    E = -7.0
    N = 41.2

    run_name = "testrun"
    output_dir = './example_data/'

    main_sentinel3('OLCI', start_time, end_time, W, S, E, N, output_dir, run_name)
    # main_sentinel3('SLSTR_SOLAR', start_time, end_time, W,S,E,N, output_dir, run_name)
    main_sentinel3('SLSTR_THERMAL', start_time, end_time, W,S,E,N, output_dir, run_name)
