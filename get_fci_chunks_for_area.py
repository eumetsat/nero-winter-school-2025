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

from satpy.resample import get_area_def

end_position_rows = [278,
                     556,
                     835,
                     1113,
                     1392,
                     1670,
                     1948,
                     2227,
                     2505,
                     2784,
                     3062,
                     3340,
                     3619,
                     3897,
                     4176,
                     4454,
                     4732,
                     5011,
                     5289,
                     5568,
                     5846,
                     6124,
                     6403,
                     6681,
                     6960,
                     7258,
                     7556,
                     7856,
                     8133,
                     8391,
                     8649,
                     8908,
                     9187,
                     9465,
                     9744,
                     10022,
                     10300,
                     10579,
                     10857,
                     11136]


def get_first_index_bigger_than(value, sorted_list):
    for index, item in enumerate(sorted_list):
        if item >= value:
            return index + 1
    return -1


def get_chunk_for_lon_lat(lon, lat):
    fci_area = get_area_def('mtg_fci_fdss_1km')
    _, row = fci_area.get_array_indices_from_lonlat(lon, lat)
    index = get_first_index_bigger_than(11136 - row, end_position_rows)
    return index


def get_chunks_for_lon_lat_bbox(lonlat_bbox):
    chunks = []
    chunks.append(get_chunk_for_lon_lat(lonlat_bbox[0], lonlat_bbox[1]))
    chunks.append(get_chunk_for_lon_lat(lonlat_bbox[0], lonlat_bbox[3]))
    chunks.append(get_chunk_for_lon_lat(lonlat_bbox[2], lonlat_bbox[1]))
    chunks.append(get_chunk_for_lon_lat(lonlat_bbox[2], lonlat_bbox[3]))
    chunks = list(range(min(chunks), max(chunks) + 1))  # Make the chunks gap free
    print(f"The chunks for area {lonlat_bbox} are {chunks}")
    return chunks


if __name__ == "__main__":
    W = 26.5
    S = 41.7
    E = 27.3
    N = 42.3
    lonlat_bbox = [W, S, E, N]
    chunks = get_chunks_for_lon_lat_bbox(lonlat_bbox)
