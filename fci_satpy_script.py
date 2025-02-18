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

import os
from datetime import datetime, timedelta

import fsspec
from satpy import Scene
from satpy.readers import find_files_and_readers, FSFile

import credentials

CACHE_SIZE_LIMIT_GB = 4
CACHE_FOLDER = "./bucket_cache/"


def run_satpy_for_files_and_area(fci_filenames, datasets, lonlat_bbox, output_dir):
    scn = Scene(filenames=fci_filenames)
    scn.load(datasets, upper_right_corner='NE')

    scn_crop = scn.crop(ll_bbox=lonlat_bbox)

    single_channel_ds = [ds for ds in datasets if ds in scn_crop and len(scn_crop[ds].shape) == 2]
    for dataset in single_channel_ds:
        scn_crop.save_dataset(dataset, filename="{start_time:%Y-%m-%dT%H%M}_mtg_fci_{name}.tif",
                              base_dir=output_dir,
                              writer='geotiff', enhance=False)

    scn_crop_r = scn_crop.resample(scn_crop.finest_area(), resampler='native', reduce_data=False)
    multi_channel_ds = [ds for ds in datasets if ds not in single_channel_ds]
    for dataset in multi_channel_ds:
        scn_crop_r.save_dataset(dataset, filename="{start_time:%Y-%m-%dT%H%M}_mtg_fci_{name}.tif",
                                base_dir=output_dir, writer='geotiff', enhance=True)

    return


def clear_cache_if_exceeds_limit(cache_storage, size_limit_gb):
    size_limit_bytes = size_limit_gb * 1024 ** 3  # Convert GB to bytes
    cache_size = sum(
        os.path.getsize(os.path.join(root, file)) for root, _, files in os.walk(cache_storage) for file in
        files)
    if cache_size > size_limit_bytes:
        print("Clearing cache")
        for root, dirs, files in os.walk(cache_storage):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))


def main_fci(input_dir, datasets, start_time, end_time, lonlat_bbox, output_dir, run_name, remote_files=False):
    # Convert start and end times to datetime objects
    start_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S")
    end_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S")

    output_dir = os.path.join(output_dir, run_name, 'Satellite_Imagery', 'FCI')
    # Iterate through each 10-minute interval between start and end times
    current_dt = start_dt
    while current_dt <= end_dt:
        print(f"Processing {current_dt}...")
        if not remote_files:
            fci_filenames = find_files_and_readers(base_dir=input_dir,
                                                   start_time=current_dt,
                                                   end_time=current_dt + timedelta(minutes=10),
                                                   reader='fci_l1c_nc',
                                                   missing_ok=True)
        else:
            fs = fsspec.filesystem(
                "simplecache",
                target_protocol="s3",
                target_options={
                    "endpoint_url": credentials.S3_ENDPOINT,
                    "key": credentials.S3_NERO_ACCESS_KEY,
                    "secret": credentials.S3_NERO_SECRET_KEY,
                },

                cache_storage=CACHE_FOLDER
            )

            fci_filenames = find_files_and_readers(base_dir=f"s3://{credentials.S3_NERO_BUCKET_NAME}/{input_dir}",
                                                   start_time=current_dt,
                                                   end_time=current_dt + timedelta(minutes=10),
                                                   reader='fci_l1c_nc',
                                                   fs=fs,
                                                   missing_ok=True)
            # manually create FSFiles for each found path and pass that to the Scene
            fci_filenames['fci_l1c_nc'] = [FSFile(fn, fs=fs) for fn in fci_filenames['fci_l1c_nc']]

        if 'fci_l1c_nc' in fci_filenames and len(fci_filenames['fci_l1c_nc']) > 0:
            print(f"Found {len(fci_filenames['fci_l1c_nc'])} filenames")
            run_satpy_for_files_and_area(fci_filenames, datasets, lonlat_bbox, output_dir)
            clear_cache_if_exceeds_limit(CACHE_FOLDER, CACHE_SIZE_LIMIT_GB)
        else:
            print("Skipping RC due to missing input files.")

        current_dt += timedelta(minutes=10)


if __name__ == "__main__":
    # should be a full 10-min time, like :00, :10, :20...
    start_time = "2024-09-16T11:00:00"
    end_time = "2024-09-16T23:59:59"

    # Define the latitude and longitude of the bounding box
    W = 26.5
    S = 41.7
    E = 27.3
    N = 42.3
    lonlat_bbox = [W, S, E, N]

    run_name = "testrun"

    # input_dir = '/tcenas/scratch/andream/fci_data_for_nero/sept_portugal/'
    input_dir = 'satellite_data/fci_data/202409_Portugal/'
    remote_files = True
    output_dir = './'

    datasets = ['nir_22', 'ir_38', 'fire_temperature']

    main_fci(input_dir, datasets, start_time, end_time, lonlat_bbox, output_dir, run_name, remote_files)
