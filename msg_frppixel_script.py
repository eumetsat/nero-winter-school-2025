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

import geopandas as gpd
import matplotlib.dates as mdates
import pandas as pd
from matplotlib import pyplot as plt


def plot_fre_from_gdfs(gdfs, current_day, output_dir):
    # Convert 'day_time' to datetime format
    gdfs["day_time"] = pd.to_datetime(gdfs["day_time"])

    # Compute Fire Radiative Energy (FRE) in Joules
    gdfs["energy_joules"] = gdfs["frp"] * 900  # 15-minute intervals (900 sec)

    # Aggregate into 30-minute intervals
    gdfs_resampled = (gdfs.resample("30min", on="day_time")["energy_joules"].sum().reset_index())

    # Convert Joules to TeraJoules (TJ)
    gdfs_resampled["energy_TJ"] = gdfs_resampled["energy_joules"] / 1e12

    # Plot the time series
    plt.figure(figsize=(10, 5))
    plt.plot(gdfs_resampled["day_time"], gdfs_resampled["energy_TJ"], marker="o", linestyle="-", color="red")
    plt.xlabel("Time")
    plt.ylabel("Fire Radiative Energy (TJ)")
    plt.title("Fire Radiative Energy Time Series (30-minute intervals)")
    ax = plt.gca()  # Get the current axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))  # Format: 'Jan 01, 2024'
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45, ha="right")
    plt.grid(True)
    output_path = os.path.join(output_dir, f"{current_day}_fre_timeseries.png")
    plt.savefig(output_path, format="png", dpi=300, bbox_inches="tight")
    print(f"Saved FRE time series plot to: {output_path}")
    plt.show(block=False)

    return


def write_gdfs_to_file(gdfs, current_day, output_dir):
    # Define the output shapefile path
    filtered_shp_path = os.path.join(output_dir, f"{current_day}_lsasaf_msg_frppixel.shp")
    os.makedirs(os.path.dirname(filtered_shp_path), exist_ok=True)
    # Write the filtered GeoDataFrame to a new shapefile
    gdfs.to_file(filtered_shp_path, driver='ESRI Shapefile')
    print(f"Filtered shapefile written to: {filtered_shp_path}")


def process_day_of_points(current_day, gdfs, output_dir):
    if gdfs is None:
        print('Initialising processing')
    elif gdfs is not None and len(gdfs) > 0:
        gdfs = pd.concat(gdfs, ignore_index=True)
        print(
            f"Processing {len(gdfs)} points for day {current_day}. "
            f"Writing shapefile and plot to {output_dir}"
        )
        write_gdfs_to_file(gdfs, current_day, output_dir)
        plot_fre_from_gdfs(gdfs, current_day, output_dir)
    else:
        print(f"Day {current_day} had no points.")


def main_lsasaf(start_time, end_time, lonlat_bbox, output_dir, run_name):
    # Convert start and end times to datetime objects
    start_time_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S")
    end_time_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S")

    lsasaf_mf2_address = "https://mf2.ipma.pt/downloads/data/lsasaf/frp/"
    file_pattern = 'LSASAF_MSG_FRP-PIXEL-ListProduct_MSG-Disk_{}.shp'

    # Generate list of times at 15-minute intervals
    current_time = start_time_dt
    output_dir = os.path.join(output_dir, run_name, 'Satellite_ActiveFires', 'MSG')
    os.makedirs(output_dir, exist_ok=True)
    current_day = None
    gdfs = None
    while current_time <= end_time_dt:

        # Check if the current day has changed
        day_of_current_time = current_time.date()
        if day_of_current_time != current_day:
            process_day_of_points(current_day, gdfs, output_dir)
            print(f"Processing new day: {day_of_current_time}")
            gdfs = []
            current_day = day_of_current_time

        download_path = (f"{lsasaf_mf2_address}/{current_time.strftime('%Y')}/"
                         f"{current_time.strftime('%m')}/{current_time.strftime('%d')}/")
        shp_path = f"{download_path}/{file_pattern.format(current_time.strftime('%Y%m%d%H%M'))}"

        try:
            # Read the shapefile using geopandas
            gdf = gpd.read_file(shp_path)
            gdf = gdf.to_crs(epsg=4326)
            # Filter the GeoDataFrame to only include points within the bounding box
            lon_min, lat_min, lon_max, lat_max = lonlat_bbox
            filtered_gdf = gdf.cx[lon_min:lon_max, lat_min:lat_max].copy()  # make a copy to avoid working on the view
            if len(filtered_gdf) == 0:
                print(f"No points left after lat-lon filtering in time {current_time}, continuing to next time")
                current_time += timedelta(minutes=15)
                continue
            else:
                print(f"Processing slot {current_time}, found {len(filtered_gdf)} points.")

            filtered_gdf.rename(columns={'value': 'frp'}, inplace=True)

            # Add a new column 'day_time' to the filtered_gdf with the same value for all items
            filtered_gdf.loc[:, 'day_time'] = current_time.strftime('%Y-%m-%d %H:%M')

            gdfs.append(filtered_gdf)
            current_time += timedelta(minutes=15)
        except Exception as e:
            current_time += timedelta(minutes=15)
            print(f"Error processing shapefile {shp_path}: {e}")

    else:
        process_day_of_points(current_day, gdfs, output_dir)
        print(f"Finished processing all times")

    return


if __name__ == "__main__":
    start_time = "2024-08-14T23:00:00"  # should be a full 15-min time, like :00, :15, :30...
    end_time = "2024-08-15T23:59:59"

    # Define the latitudes and longitudes of the bounding box
    W = 23.3
    S = 37.8
    E = 24.5
    N = 38.5
    lonlat_bbox = [W, S, E, N]

    run_name = "testrun"

    output_dir = './test/'
    main_lsasaf(start_time, end_time, lonlat_bbox, output_dir, run_name)
