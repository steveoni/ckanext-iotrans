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

ckanext-iotrans creates the following CKAN actions - both will only work for admin users:

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

When running `to_file`, data is first streamed from the datastore into a `csv` stored on disk. All subsequent files created will be created from this "dump" file. When writing to new output files, content is streamed from the "dump" file into each output file (to reduce memory usage). Streaming from CKAN is done via multiple sequential calls to CKAN's `datastore_search` action.

Processing to convert files to another format, or transform coordinates from one Coordinate Reference System to another, are done in memory on one chunk of the data at a time - `ckanext-iotrans` never loads an entire file into memory.

### Geometric Data

`ckanext-iotrans` identifies spatial dataas anything containing a `geometry` attribute. A `geometry` attribute's value should be structured as follows:

```json
    "type": "Some Geometry Type", 
    "coordinates": [X Coordinate, Y Coordinate]
```
So, for example:
```json
    "type": "Point", 
    "coordinates": [-79.156501959987, 43.232603612123]
```

The following geometry types are currently accepted:
* Point
* LineString
* Polygon
* MultiPoint
* MultiLineString
* MultiPolygon

To avoid mixed geometry types in a single output file, all non-Multi geometry types are converted to their Multi counterparts (ex: Point to MultiPoint).

### Shapefiles

Shapefiles get treated differently than other file formats.

By default, shapefiles cannot have column names longer than 10 characters. To adapt to this, `ckanext-iotrans` will (for shapefiles with at least one column name longer than 10 characters) truncate each column name to its first 7 characters, and then concatenate to it an incrementing integer. The integer is added to maintain that all columns have unique names.

The mapping from the original filename to its newly truncated one in the output shapefile is stored in a `.txt` that is zipped inside the shapefile with the rest of its components.

For example:

| Input Column Name | Output Column Name   |
| ---------------   | ------------- |
| ID                | ID1           |
| LOCATION_NAME     | LOCATIO2      |
| LOCATION_ID       | LOCATIO3      |

## Contribution

Please contact opendata@toronto.ca