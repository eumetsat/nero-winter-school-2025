# Copyright (C) 2025 EUMETSAT
# 
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.


import openeo
from pathlib import Path
from os import chmod


def main_sentinel2(start_date, end_date, W, S, E, N, bands, output_dir, run_name):

    download_dir = Path(output_dir) / run_name / "Satellite_Imagery" / "S2_DAILY"

    # chmod('/home/jovyan/.local/share/openeo-python-client/refresh-tokens.json', 0o600)
    connection = openeo.connect(url="openeo.dataspace.copernicus.eu")
    connection.authenticate_oidc()

    # Ensure start_date and end_date contain only date (no time)
    start_date = start_date.split("T")[0] if "T" in start_date else start_date
    end_date = end_date.split("T")[0] if "T" in end_date else end_date

    
    datacube = connection.load_collection(
        "SENTINEL2_L2A",
        bands=bands,
        temporal_extent=(start_date, end_date),
        spatial_extent={
            "west": W,
            "south": S,
            "east": E,
            "north": N,
            "crs": "EPSG:4326",
        },
        max_cloud_cover=100,
    )
    
    job = datacube.save_result(format="GTiff")
    
    job = job.execute_batch(title="Slice of S2 data")
    print(f'Downloaded files:')
    
    results = job.get_results()
    paths = results.download_files(download_dir)
    for path in paths:
        print(f'File: {path}')

if __name__ == "__main__":
    start_time = "2024-09-20T00:00:00"
    end_time = "2024-09-22T23:59:59"

    # Define the latitude and longitude of the bounding box
    W = -8.8
    S = 40.6
    E = -7.4
    N = 40.9
    
    bands = ["B08", "B04", "B03", "B02", "B12"]

    run_name = "aveiro_penalva"
    output_dir = './ref_data/'

    main_sentinel2(start_time, end_time, W, S, E, N, bands, output_dir, run_name)
