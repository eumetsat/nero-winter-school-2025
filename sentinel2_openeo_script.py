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

def main_sentinel2(start_date, end_date, W,S,E,N, bands, output_dir, run_name):
    download_dir = Path(output_dir) / run_name / "Satellite_Imagery" / "S2_DAILY"

    chmod('/home/jovyan/.local/share/openeo-python-client/refresh-tokens.json', 0o600)
    connection = openeo.connect(url="openeo.dataspace.copernicus.eu")
    connection.authenticate_oidc()
    
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
    S = 38.017263
    W = 23.806
    E = 23.994827
    N = 38.267837

    start_date = "2024-08-07"
    end_date = "2024-08-18" #time 00:00
    
    bands = ["B08", "B04", "B03", "B02", "B12"]

    run_name = "testrun"

    output_dir = './'

    main_sentinel2(start_date, end_date, W, S, E, N, bands, output_dir, run_name)
