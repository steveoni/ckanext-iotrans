# ckanext-iotrans

This CKAN extension lets you convert datastore resources into files of various formats and, for spatial data, Coordinate Reference Systems.

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

- **source_epsg**: source EPSG of resource ID, if data is spatial, as an integer

- **target_epsgs**: list of desired EPSGs of output files, if data is spatial, as integers

- **target_formats**: list of desired file formats as strings. ex: `["csv", "xml", "json"]` 

| Spatial Formats | Non Spatial Formats   |
| --------------- | ------------- |
| CSV             | CSV           |
| GEOJSON         | JSON          |
| GPKG            | XML           |
| SHP             |               |

#### Outputs:

Writes desired files to folder in /tmp, and returns a list of filepaths where the outputs are stored on disk

### `prune`

#### Inputs:

- **path**: path to file or directory in `/tmp` to delete

#### Outputs:

Removes file or directory, as long as its in `/tmp` directory 


## Details

### Memory and Disk Use

When running `to_file`, data is first streamed from the datastore into a `csv` stored on disk. All subsequent files created will be created from this "dump" file. When writing to new output files, content is streamed from the "dump" file into each output file, so to reduce memory usage, and allow converting large files. Streaming from CKAN is done via multiple sequential calls to CKAN's `datastore_search` action.

Processing to convert files to another format, or transform coordinates from one Coordinate Reference System to another, are done on chunks of the data at a time - `ckanext-iotrans` never loads an entire file into memory.

