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

# inspired by https://github.com/nasa/HLS-Data-Resources/blob/main/python/tutorials/HLS_Tutorial.ipynb
import os
from datetime import datetime, timedelta

import earthaccess
import geopandas as gpd
import rioxarray as rxr
import xarray as xr
from osgeo import gdal
from shapely.geometry import Polygon

import warnings

warnings.filterwarnings("ignore", category=RuntimeWarning, module="dask")

import credentials

# TODO merge granules from same orbit into one raster

# GDAL configurations used to successfully access LP DAAC Cloud Assets via vsicurl
gdal.SetConfigOption('GDAL_HTTP_COOKIEFILE', '~/cookies.txt')
gdal.SetConfigOption('GDAL_HTTP_COOKIEJAR', '~/cookies.txt')
gdal.SetConfigOption('GDAL_DISABLE_READDIR_ON_OPEN', 'EMPTY_DIR')
# keep also shapefile extensions to not disturb the lsasaf script
gdal.SetConfigOption('CPL_VSIL_CURL_ALLOWED_EXTENSIONS', 'TIF,SHP,SHX,DBF,PRJ')
gdal.SetConfigOption('GDAL_HTTP_UNSAFESSL', 'YES')
gdal.SetConfigOption('GDAL_HTTP_MAX_RETRY', '10')
gdal.SetConfigOption('GDAL_HTTP_RETRY_DELAY', '0.5')


# Define function to scale
def scaling(band):
    scale_factor = band.attrs['scale_factor']
    band_out = band.copy()
    band_out.data = band.data * scale_factor
    band_out.attrs['scale_factor'] = 1
    return (band_out)


def compute_date(year, day_of_year):
    # Start with the first day of the year
    start_of_year = datetime(year, 1, 1)
    # Add the day offset
    resulting_date = start_of_year + timedelta(days=day_of_year - 1)
    return resulting_date.strftime("%Y-%m-%d")


def get_rgb_array(red: xr.DataArray, green: xr.DataArray, blue: xr.DataArray):
    """
    Combines three 2D xarray DataArrays (Red, Green, Blue) into an RGB raster
    while preserving spatial metadata, and saves it as a GeoTIFF.

    Parameters:
        red (xr.DataArray): 2D array representing the red channel (0-1 or 0-255).
        green (xr.DataArray): 2D array representing the green channel (0-1 or 0-255).
        blue (xr.DataArray): 2D array representing the blue channel (0-1 or 0-255).
    """

    # Ensure input data is in 8-bit (0-255)
    red_8bit = (red * 255).clip(0, 255).astype("uint8")
    green_8bit = (green * 255).clip(0, 255).astype("uint8")
    blue_8bit = (blue * 255).clip(0, 255).astype("uint8")

    # Stack into an RGB array with a new "band" dimension
    rgb = xr.concat([red_8bit, green_8bit, blue_8bit], dim="band")

    # Assign band names (1=Red, 2=Green, 3=Blue)
    rgb = rgb.assign_coords(band=[1, 2, 3])

    # Set spatial attributes from one of the original arrays
    rgb.rio.write_crs(red.rio.crs, inplace=True)
    rgb.rio.write_transform(red.rio.transform(), inplace=True)

    return rgb


def main_earthdata(start_time, end_time, lonlat_bbox, output_dir, run_name, composites_dict):
    earthaccess.login(persist=True)

    # Create a Polygon representing the bounding box
    min_lon, min_lat, max_lon, max_lat = lonlat_bbox
    bounding_box = Polygon([
        (min_lon, min_lat),  # Bottom-left
        (min_lon, max_lat),  # Top-left
        (max_lon, max_lat),  # Top-right
        (max_lon, min_lat),  # Bottom-right
        (min_lon, min_lat)  # Closing the polygon
    ])

    # Create a GeoDataFrame with the bounding box
    geo_df = gpd.GeoDataFrame({'geometry': [bounding_box]}, crs="EPSG:4326")  # WGS84 coordinate system


    results = earthaccess.search_data(
        short_name=['HLSL30', 'HLSS30'],
        bounding_box=tuple(lonlat_bbox),
        temporal=(start_time, end_time),
    )
    if len(results) == 0:
        print("No results found for query. Exiting.")
        return
    else:
        print(f"Found {len(results)} results for query.")
    
    hls_results_urls = [granule.data_links() for granule in results]

    for composite_name, bands_dict in composites_dict.items():
        print(f"Generating {composite_name}")
        for j, granule_bands_urls in enumerate(hls_results_urls):

            if granule_bands_urls[0].split('/')[4] == 'HLSS30.020':
                instrument_name = 'Sentinel2'
                bands = bands_dict['Sentinel2']

            elif granule_bands_urls[0].split('/')[4] == 'HLSL30.020':
                instrument_name = 'Landsat89'
                bands = bands_dict['Landsat89']
            else:
                print("WARNING: did not recognise file, skipping. File name: ", granule_bands_urls[0])
                continue

            datestr = granule_bands_urls[0].split('/')[-1].split('.')[3].split('T')
            tile_id = granule_bands_urls[0].split('/')[-1].split('.')[2]
            output_name = os.path.join(output_dir, run_name, 'Satellite_Imagery', 'Landsat-Sentinel2',
                                       f"{compute_date(int(datestr[0][0:4]), int(datestr[0][-3:]))}T{datestr[1]}_"
                                       f"{composite_name}_{instrument_name}_{tile_id}_cropped.tif")
            print(f"Preparing {output_name}")
            # Check if file already exists in output directory, if yes--skip that file and move to the next observation
            if os.path.exists(output_name):
                print(
                    f"{output_name} has already been processed and is available in this directory, moving to next file.")
                continue

            band_links = []
            for a in granule_bands_urls:
                if any(b in a for b in bands):
                    band_links.append(a)

            # Use vsicurl to load the data directly into memory (be patient, may take a few seconds)
            chunk_size = dict(band=1, x=512, y=512)  # Tiles have 1 band and are divided into 512x512 pixel chunks
            for e in band_links:
                if e.rsplit('.', 2)[-2] == bands[0]:
                    red_beam = rxr.open_rasterio(e, chunks=chunk_size, masked=True).squeeze('band', drop=True)
                    red_beam.attrs['scale_factor'] = 0.0001  # hard coded the scale_factor attribute
                elif e.rsplit('.', 2)[-2] == bands[1]:
                    green_beam = rxr.open_rasterio(e, chunks=chunk_size, masked=True).squeeze('band', drop=True)
                    green_beam.attrs['scale_factor'] = 0.0001  # hard coded the scale_factor attribute
                elif e.rsplit('.', 2)[-2] == bands[2]:
                    blue_beam = rxr.open_rasterio(e, chunks=chunk_size, masked=True).squeeze('band', drop=True)
                    blue_beam.attrs['scale_factor'] = 0.0001  # hard coded the scale_factor attribute

            fsUTM = geo_df.to_crs(red_beam.spatial_ref.crs_wkt)

            # Crop to our ROI and apply scaling and masking
            red_beam_cropped = red_beam.rio.clip(fsUTM.geometry.values, fsUTM.crs, all_touched=True)
            green_beam_cropped = green_beam.rio.clip(fsUTM.geometry.values, fsUTM.crs, all_touched=True)
            blue_beam_cropped = blue_beam.rio.clip(fsUTM.geometry.values, fsUTM.crs, all_touched=True)

            print('Cropped')

            # Fix Scaling
            red_beam_cropped_scaled = scaling(red_beam_cropped)
            green_beam_cropped_scaled = scaling(green_beam_cropped)
            blue_beam_cropped_scaled = scaling(blue_beam_cropped)

            rgb = get_rgb_array(red_beam_cropped_scaled, green_beam_cropped_scaled, blue_beam_cropped_scaled)

            os.makedirs(os.path.dirname(output_name), exist_ok=True)
            rgb.rio.to_raster(raster_path=output_name, driver='COG')

            print(f"Processed file {j + 1} of {len(hls_results_urls)}")
    print("Processing has finished")
    return


if __name__ == "__main__":
    start_time = "2024-09-15T12:00:00"
    end_time = "2024-09-20T23:59:59"

    # Define the latitudes and longitudes of the bounding box
    W = -9.3
    S = 40
    E = -7.0
    N = 41.2
    lonlat_bbox = [W, S, E, N]

    run_name = "testrun"

    output_dir = '/tcenas/scratch/andream/firedata/'

    composites_dict = {
        'SWIR-composite': {
            'Sentinel2': ['B12', 'B8A', 'B04'],
            'Landsat89': ['B07', 'B05', 'B04']
        }
    }

    main_earthdata(start_time, end_time, lonlat_bbox, output_dir, run_name, composites_dict)
