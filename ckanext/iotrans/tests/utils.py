import fiona
import json
import os
from typing import Tuple, Callable
import csv
import filecmp
from fiona import Collection, Geometry


def _get_csv_comparator(
    compare_row: Callable, ignore_header=True
) -> Callable[[str, str], bool]:

    def _compare_fn(csv_path_1: str, csv_path_2: str) -> bool:
        with open(csv_path_1, "r") as csv_1_file:
            csv_1 = csv.reader(csv_1_file)
            with open(csv_path_2, "r") as csv_2_file:
                csv_2 = csv.reader(csv_2_file)
                header = True

                for row1, row2 in zip(csv_1, csv_2):
                    if ignore_header and header:
                        header = False
                        continue
                    elif not compare_row(row1, row2):
                        return False

                # if either csv1 or csv2 still has rows, return False
                try:
                    next(csv_1)
                    return False
                except StopIteration:
                    pass
                try:
                    next(csv_2)
                    return False
                except StopIteration:
                    pass
        return True

    return _compare_fn


def _compare_json_small(json_path_1, json_path_2) -> Tuple[bool, str]:
    with open(json_path_1) as json_file_1:
        json_1 = json.load(json_file_1)

    with open(json_path_2) as json_file_2:
        json_2 = json.load(json_file_2)

    if json_1 != json_2:
        return False
    return True


def fiona_collections_eq(
    col_1: Collection, col_2: Collection, threshold: float
) -> bool:
    """fion_collections_eq

    :param col_1: a fiona Collection
    :type col_1: Collection
    :param col_2: a fiona Collection to compare to
    :type col_2: Collection
    :param threshold: maximum  bound (inclusive) for 2 coordinate components to be off
      from one another for the collections to still be considered equivalent.
    :type threshold: float
    :return: True if col_1 is 'close enough' to col_2 based on the threshold
    :rtype: bool
    """
    if (
        (col_1.schema != col_2.schema)
        or (col_1.crs != col_2.crs)
        or len(col_1) != len(col_2)
    ):
        return False

    for feat_1, feat_2 in zip(col_1, col_2):

        if not fiona_geoms_eq(feat_1["geometry"], feat_2["geometry"], threshold):
            return False

        if feat_1["properties"] != feat_2["properties"]:
            return False
    return True


def fiona_geoms_eq(geom_1: Geometry, geom_2: Geometry, threshold: float) -> bool:
    """fiona_geoms_eq

    :param geom_1: a fiona Geometry
    :type geom_1: Geometry
    :param geom_2: a fiona Geometry to compare to
    :type geom_2: Geometry
    :param threshold: maximum  bound (inclusive) for 2 coordinate components to be off
      from one another for the geometries to still be considered equivalent.
    :type threshold: float
    :return: True if geom_1 is 'close enough' to geom_2 based on the threshold
    :rtype: bool
    """
    eq_attributes = ["type", "geometries"]
    for attr in eq_attributes:
        if geom_1[attr] != geom_2[attr]:
            return False
    if len(geom_1["coordinates"]) != len(geom_2["coordinates"]):
        return False

    for (coord_1_x, coord_1_y), (coord_2_x, coord_2_y) in zip(
        geom_1["coordinates"], geom_2["coordinates"]
    ):
        if (abs(coord_1_x - coord_2_x) > threshold) or (
            abs(coord_1_y - coord_2_y) > threshold
        ):
            return False

    return True


def geographic_files_eq(
    file_path_1: str, file_path_2: str, threshold: float = 0.98
) -> bool:
    """geographic_files_eq

    :param file_path_1: path to a shp or shx file
    :type file_path_1: str
    :param file_path_2: path to a shp or shx file
    :type file_path_2: str
    :param threshold: fiona collections compare threshold, defaults to 0.98
    :type threshold: float, optional
    :return: true if geographic files are roughly equivalent (based on threshold)
    :rtype: bool
    """
    with fiona.open(file_path_1) as file_1, fiona.open(file_path_2) as file_2:
        return fiona_collections_eq(file_1, file_2, threshold)


csv_rows_eq = ("csv", _get_csv_comparator(lambda a, b: a == b))
json_small = ("json", _compare_json_small)
geojson_small = ("geojson", _compare_json_small)
xml_eq = ("xml", filecmp.cmp)

CORRECT_DIR_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "correct_files"
)
TEST_TMP_PATH = os.path.join("tmp", "iotrans_test_folder")
