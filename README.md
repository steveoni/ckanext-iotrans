# ckanext-iotrans

This CKAN extension adds new CKAN actions useful for converting datastore resources into filestore resources of different file formats and, when the data is geospatial, Coordinate Reference Systems.

This extension is intended for use cases that require download of datastore resources in multiple formats of Coordinate Reference Systems aside from CKAN's built in formats.

This extension leverages the [Fiona](https://pypi.org/project/Fiona/) Python library.

## Requirements

This extension only works if the [CKAN Datastore Extension](https://docs.ckan.org/en/2.9/maintaining/datastore.html) is active

Compatibility with core CKAN versions:

| CKAN version    | Compatible?   |
| --------------- | ------------- |
| 2.6 and earlier | not tested    |
| 2.7             | not tested    |
| 2.8             | not tested    |
| 2.9             | yes    |

## Usage

ckanext-iotrans creates the following CKAN action:

### `to_file`

#### Inputs:

- **resource_id**: CKAN datastore resource ID

- **source_epsg**: source EPSG of resource ID, if data is spatial

- **target_epsgs**: list of desired EPSGs of output files, if data is spatial

- **target_formats**: list of desired file formats. Currently, `JSON`, `CSV` and `XML` are supported for non spatial data, while `SHP`, `GPKG`, `GEOJSON` and `CSV` are supported for spatial data

#### Outputs:

Writes desired files to folder in /tmp, and returns a list of filepaths where the outputs are stored on disk

