# Copyright (C) 2025 EUMETSAT
# 
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.


from eocanvas import API, Credentials
from eocanvas.api import Input, Config, ConfigOption
from eocanvas.datatailor.chain import Chain
from eocanvas.processes import DataTailorProcess

from hda import Client

from concurrent.futures import ThreadPoolExecutor

MAX_QUOTA = 3

from time import sleep
from pathlib import Path

import rasterio
from rasterio.mask import mask
from shapely.geometry import box
import os

import credentials


def get_query_and_chain(instrument, start_time, end_time, W, S, E, N):
    query = {
        "dtstart": start_time + ".000Z",
        "dtend": end_time + ".000Z",
        "bbox": [
            W,
            S,
            E,
            N
        ],
        "timeliness": "NT",
    }

    if instrument == 'SLSTR_SOLAR':
        query["dataset_id"] = "EO:EUM:DAT:SENTINEL-3:SL_1_RBT___"
        query["orbitdir"] = "DESCENDING"
        chain_path = "input_graphs/slstr_solar_subset.yaml"
    elif instrument == 'SLSTR_THERMAL':
        query["dataset_id"] = "EO:EUM:DAT:SENTINEL-3:SL_1_RBT___"
        chain_path = "input_graphs/slstr_thermal_subset.yaml"
    elif instrument == 'OLCI':
        query["dataset_id"] = "EO:EUM:DAT:SENTINEL-3:OL_1_EFR___"
        chain_path = "input_graphs/olci_subset.yaml"
    else:
        return None

    return (query, chain_path)


def exec_data_tailor_process(url, chain, download_dir):
    print(f"Starting Data Tailor for product at {url}")
    inputs = Input(key="img1", url=url)
    process = DataTailorProcess(epct_chain=chain, epct_input=inputs)
    process.run(download_dir=download_dir)
    return f"Finishing Data Tailor for product at {url}"
    sleep(1)  # Wait for one sec to finish


def search_output_filename_in_eocanvas_log(log):
    filename = None
    for line_number, entry in enumerate(log, start=1):
        if 'output-product' in entry.message:  # case-insensitive search
            filename = Path(entry.message.split(' ')[-1]).name
            break
    return filename


def get_eocanvas_log(api, job_index):
    job_id = api.get_jobs()[job_index].job_id
    log = api.get_job_logs(job=job_id)
    return log


def get_results_path_list(url_list, download_dir):
    result_paths = []
    api = API()
    for job_index in range(len(url_list)):
        log = get_eocanvas_log(api, job_index)
        file = search_output_filename_in_eocanvas_log(log)
        if file is not None:
            result_paths.append(Path(download_dir) / file)

    return result_paths


def subset_and_overwrite_geotiff(input_tiff, bounding_box):
    # Input GeoTIFF path
    temp_tiff = input_tiff.parent.absolute() / "temp_subset.tif"

    # Create a geometry for the bounding box
    bbox_geom = [box(*bounding_box)]

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
    download_dir = Path(output_dir) / run_name / 'Satellite_Imagery' / instrument

    c = Credentials(username=credentials.WEKEO_USERNAME, password=credentials.WEKEO_PASSWORD)
    c.save()
    c = Client()

    (query, chain_path) = get_query_and_chain(instrument, start_time, end_time, W, S, E, N)

    print(f'Querying data...')
    r = c.search(query)
    url_list = r.get_download_urls()

    id_list = [result['id'] for result in r.results]
    print(f'The following granules will be processed:')
    print('\n'.join(id_list))

    chain = Chain.from_file(chain_path)

    with ThreadPoolExecutor(max_workers=MAX_QUOTA) as executor:

        futures = [executor.submit(exec_data_tailor_process, url, chain, download_dir) for url in url_list]

        # Get the results as they complete
        for future in futures:
            print(future.result())

    bounding_box = [W, S, E, N]

    result_paths = get_results_path_list(url_list, download_dir)
    for input_tiff in result_paths:
        subset_and_overwrite_geotiff(input_tiff, bounding_box)

    return


if __name__ == "__main__":
    start_time = "2024-09-14T00:00:00"
    end_time = "2024-09-20T23:59:59"

    # Define the latitude and longitude of the bounding box
    W = -8.8
    S = 40.6
    E = -7.4
    N = 40.9

    run_name = "aveiro_penalva"
    output_dir = './ref_data/'

    # main_sentinel3('SLSTR_SOLAR', start_time, end_time, W,S,E,N, output_dir, run_name)
    # main_sentinel3('SLSTR_THERMAL', start_time, end_time, W,S,E,N, output_dir, run_name)
    main_sentinel3('OLCI', start_time, end_time, W, S, E, N, output_dir, run_name)
