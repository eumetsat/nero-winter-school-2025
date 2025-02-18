# Copyright (C) 2025 EUMETSAT
# 
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.


import os
import json
import datetime
import shutil
import eumdac
import requests

import geopandas as gpd
import pandas as pd
from shapely import wkt
from shapely.geometry import box
from pathlib import Path

import credentials

def get_eumdac_access_token(credentials=credentials):
    consumer_key = credentials.EUMDAC_CONSUMER_KEY
    consumer_secret = credentials.EUMDAC_CONSUMER_SECRET
    
    credentials = (consumer_key, consumer_secret)
    token = eumdac.AccessToken(credentials) 
    
    try:
        print(f"This token '{token}' expires {token.expiration}")
        return token
    except requests.exceptions.HTTPError as exc:
        print(f"Error when tryng the request to the server: '{exc}'")

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

def search_spatially_temporally_for_products(collection, start, end, bounding_box):
    # space/time filter the collection for products
    products = collection.search(
        bbox=bounding_box,
        dtstart=start, 
        dtend=end)
    
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



def download_given_file_from_product(datastore, requested_file, result_collection_search, collectionID, download_dir):
    selected_product = datastore.get_product(product_id=str(result_collection_search), collection_id=collectionID)
    for entry in selected_product.entries:
        if requested_file in entry:
            try:
                target_filename = str(selected_product).split('.')[0] + "_" + requested_file
                with selected_product.open(entry=entry) as fsrc, open(os.path.join(download_dir, target_filename), mode='wb') as fdst:
                    print(f'Downloading {fsrc.name}.')
                    shutil.copyfileobj(fsrc, fdst)
                    print(f'Download of file {fsrc.name} from {selected_product} finished.')
            except eumdac.collection.CollectionError as error:
                print(f"Error related to the collection: '{error.msg}'")
            except eumdac.product.ProductError as error:
                    print(f"Error related to the product: '{error.msg}'")
            except requests.exceptions.RequestException as error:
                print(f"Unexpected error: {error}")
    return

def get_all_csvs_in_given_path(dir_path):
    #List all the files in a given path. Those files will be later combined in one shapefile
    csv_files = [os.path.join(dir_path,f) for f in os.listdir(dir_path) if 'csv' in f]
    return csv_files

def convert_s3_frp_csv_to_geodataframe(csv_file_path, header_lines=19):
    # Read the CSV file
    df = pd.read_csv(csv_file_path, skiprows=header_lines)

    # Convert the DataFrame to a GeoDataFrame
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df['lon(deg)'], df['lat(deg)']))

    # Set the CRS to WGS84 (EPSG:4326)
    gdf.set_crs(epsg=4326, inplace=True)

    return gdf

def subset_geodataframe_spatially(gdf, bounding_box):
    # Filter points within the bounding_box
    bounding_box_tuple = tuple(map(float, bounding_box.split(',')))
    bbox = box(*bounding_box_tuple)
    gdf = gdf[gdf.within(bbox)]
    return gdf

def export_and_subset_many_s3_frp_csvs_to_single_shp(csv_files, bounding_box, requested_file):
    # Initialize an empty list to store GeoDataFrames
    gdfs = []
    
    for csv_file in csv_files:
        gdf = convert_s3_frp_csv_to_geodataframe(csv_file)
        subset_gdf = subset_geodataframe_spatially(gdf, bounding_box)

        # Append the GeoDataFrame to the list
        print(subset_gdf)
        gdfs.append(subset_gdf)

    # Concatenate all GeoDataFrames into a single GeoDataFrame
    big_gdf = pd.concat(gdfs, ignore_index=True)

    # Combine day and time columns, skip seconds
    big_gdf['day_time'] = big_gdf['day'] + ' ' + big_gdf['time'].str.slice(0, 5)

    folder_path = Path(csv_file).parent
    # Save all columns as properties in the shapefile
    shapefile_path = os.path.join(folder_path, os.path.basename(requested_file).split('.')[0] + '.shp')
    big_gdf.to_file(shapefile_path, driver='ESRI Shapefile')
    
    print(f"Shapefile saved to {shapefile_path}")
    return

def main_sentinel3_frp(start_time, end_time, bounding_box, output_dir, run_name):
    download_dir = Path(output_dir) / run_name / "Satellite_ActiveFires" / "S3_FRP"
    # Create a download directory for our downloaded products
    os.makedirs(download_dir, exist_ok=True)
    
    # collection ID for SLSTR L2 FRP
    collectionID = 'EO:EUM:DAT:0417'
    requested_file = 'FRP_MWIR1km_standard.csv'

    token = get_eumdac_access_token()
    # create data store object
    datastore = eumdac.DataStore(token)
    collection = get_eumdac_collection(datastore, collectionID)

    products = search_spatially_temporally_for_products(collection, start_time, end_time, bounding_box)


    for product in products:
        download_given_file_from_product(datastore, requested_file, product, collectionID, download_dir)

    csv_files = get_all_csvs_in_given_path(download_dir)
    export_and_subset_many_s3_frp_csvs_to_single_shp(csv_files, bounding_box, requested_file)

if __name__ == "__main__":
    start_time = datetime.datetime(2024, 8, 11)
    end_time = datetime.datetime(2024, 8, 14)
    W = 23.806
    S = 38.017263
    E = 23.994827
    N = 38.267837
    bounding_box = f'{W}, {S}, {E}, {N}' # West, South, East, North
    
    run_name = "testrun"

    output_dir = './'

    main_sentinel3_frp(start_time, end_time, bounding_box, output_dir, run_name)


        