# wx-models-to-geotiff

## Prerequisites
* Linux (due to using SIGALRMs to kill faulty connections to NCEP -- otherwise this could be used on Windows. Good opportunity for a refactor.)
* Python 3
* GDAL/OGR with python bindings
* PostgreSQL + psycopg2
* Docker (if you want to use the NBM model and don't want to build GDAL/OGR from source)

## What's Here
`./db` has the Postgres table schemas that the script uses to keep track of things.

`./scripts` contains python code and JSON configuration for grabbing weather model data off the internet (via NCEP NOMADS), processing it, and organizing it on the file system.  This script is designed to be run on a regular basis from, for example, a cron job. See the readme in this folder for more information about the script.

`./map` contains the MapServer mapfile and some auxiliary data. The use of a mapfile has been deprecated but it is still usable with a bit of tweaking.

The primary config (all the weather models and desired vars/levels) is found at `./scripts/config.json`. Only a small number of the many dozen available models have been included to help get you started, but there is a very reasonable spread across a few popular models.

The entry point of the script is `./scripts/wxdata.py`.

## What's Not Here
The actual API to make this data accessible from a server. Using the run_status and models postgres table, and whatever organization system suits your fancy (PostGIS, flatfile, COG, etc.), you can distribute this data in its raster form or make it queryable. The primary author of this code is using a NodeJS API that queries the data using Geotiff.js.