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

# inspired by https://firms.modaps.eosdis.nasa.gov/content/academy/data_api/firms_api_use.html
import os
from datetime import datetime, timedelta

import geopandas as gpd
import pandas as pd

import credentials


def export_firms_df_to_shapefile(df, day, instrument, output_folder):
    # Convert the DataFrame to a GeoDataFrame
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df['longitude'], df['latitude']), crs="EPSG:4326")
    # Set the CRS to WGS84 (EPSG:4326)
    gdf.set_crs(epsg=4326, inplace=True)
    gdf['day_time'] = (gdf['acq_date'] + ' ' + gdf['acq_time'].astype(str).str.zfill(4).str[:2]
                       + ':' + gdf['acq_time'].astype(str).str.zfill(4).str[2:])
    # Save all columns as properties in the shapefile
    output_filename = f"{day}_firms_active_fires_{instrument}.shp"
    shapefile_path = os.path.join(output_folder, output_filename)
    os.makedirs(os.path.dirname(shapefile_path), exist_ok=True)
    gdf.to_file(shapefile_path, driver='ESRI Shapefile')
    print(f"Saved {shapefile_path}")


def call_firms(day, lonlat_bbox, output_folder):
    print(f"\nQuerying FIRMS for day {day}")
    instruments_dict = {
        'MODIS': ['MODIS_NRT', 'MODIS_SP'],
        'VIIRS': ['VIIRS_SNPP_NRT', 'VIIRS_NOAA20_NRT', 'VIIRS_NOAA21_NRT', 'VIIRS_SNPP_SP'],
    }
    for instrument, instrument_prod_types in instruments_dict.items():
        print(f"Searching for {instrument} data...")
        instrument_df = None
        for instrument_prod_type in instrument_prod_types:
            area_url = (f'https://firms.modaps.eosdis.nasa.gov/api/area/csv/{credentials.FIRMS_MAP_KEY}/'
                        f'{instrument_prod_type}/'
                        f'{lonlat_bbox[0]},{lonlat_bbox[1]},{lonlat_bbox[2]},{lonlat_bbox[3]}/1/{day}')
            df = pd.read_csv(area_url)
            if len(df) == 0:
                print(f"No data found for {area_url}")
            else:
                print(f"Found {len(df)} data points for {area_url}")
                if instrument_df is None:
                    instrument_df = df
                else:
                    instrument_df = pd.concat([instrument_df, df], ignore_index=True)

        if instrument_df is None:
            print(f"--> No data found for {instrument} on {day}")
        else:
            print(f"--> Found {len(instrument_df)} data points for {instrument} on {day}. Exporting.")
            export_firms_df_to_shapefile(instrument_df, day, instrument, output_folder)

    return


def main_firms(start_time, end_time, lonlat_bbox, output_folder, run_name):
    # Generate range of days between start_time and end_time
    start = datetime.fromisoformat(start_time if "T" in start_time else f"{start_time}T00:00:00")
    end = datetime.fromisoformat(end_time if "T" in end_time else f"{end_time}T00:00:00")
    current = start
    output_folder = os.path.join(output_folder, run_name, 'Satellite_ActiveFires', 'MODIS-VIIRS')
    while current <= end:
        day = current.strftime('%Y-%m-%d')
        call_firms(day, lonlat_bbox, output_folder)
        current += timedelta(days=1)



if __name__ == "__main__":

    start_time = "2024-09-15T00:00:00"
    end_time = "2024-09-20T23:59:59"

    output_folder = "./test/"

    run_name = "testrun"

    W = 26.5
    S = 41.7
    E = 27.3
    N = 42.3
    lonlat_bbox = [W, S, E, N]
    main_firms(start_time, end_time, lonlat_bbox, output_folder, run_name)
