# Model Processing Overview
 1. Run through the config and see which models are active.
 2. Cross-reference that model, and its run times, with its status on Postgres -> models.
 3. If the model is not currently processing, and the last processed timestamp is older than the latest available model, begin the process.
 4. Pull down either in the individual bands or the full GRIB file for the model, and extract the desired data.
 5. Create a master GeoTIFF (it doesn't exist) with that band's name and model timestamp, and place the extracted data in the band appropriate for the forecast hour. The first forecast hour would write to band 1 of the GeoTIFF, for instance.

## Basic Config
This section goes over the options available in the `config` section of the `config.json` file.

### postgres
You can supply the `host`, `db`, and `user` that will be used to connect to postgres. You will need an environment variable or `.pgpass` file to supply the password.

### bounds
You can define various bands to clip weather models to. For instance, you may want to limit high resolution models to a smaller area than low reesolution models. You can supply a key with the name of the bounds which references an object with keys `top`, `right`, `bottom`, and `left` in WGS84 coordinates.

Example:
```        
"bounds": {
  "colorado": {
    "top": "42",
    "right": "-100",
    "bottom": "36",
    "left": "-111"
  },
  "northAmerica": {
    "top": "80",
    "right": "-60",
    "bottom": "20",
    "left": "-180"
  }
},
```

### logLevels
An array defining which level of logging will be shown. Some of these will be written remotely to the `logs` table of the database. An combination of `INFO`, `DEBUG`, `WARN`, `NOTICE`, and `ERROR` can be specified.

### tempDir
The directory (relative to the script location) that model files will be written to before processing. The files will be removed when processing is complete.

### mapfileDir
The directory (relative to the script location) that the final GeoTIFF outputs will be written to. These will be organized into their own folders per model.

### resampling
The resampling algorithm used to rescale the model data during the warping/conversion process. Accepts typical GDAL resampling types.

### version (not implemented)
The version of the config file.

### retentionDays
How many days old model runs, logs, and run information will be retained on the filesystem and in the database before being deleted.

### maxThreads
The number of simultaneous threads allowed at once. More threads allows more models to be processed at the same time, but increases instability.

### pausedResumeMinutes
The number of minutes between checks for a paused model. This is so that if a model is paused, it wont immediately check if the next forecast hour is available on the next processing loop, or which may result in pinging the NCEP servers quite a few times in a minute, which NCEP has asked customers to not do.

### maxRetriesPerStep
The number of times to attempt to retry a failed processing step. After this limit, the script moves onto the next step and the failed forecast hour will likely be corrupt and unusable.

## levelMaps
The `levelMaps` section of `config.json` defines mapping for looking up levels in both `.idx` files and in GRIB metadata itself. For instance, looking for the `surface` level in an `.idx` file requires looking for the word `surface`, defined as the `idxName` of the level map. In GRIB metadata, the same level is represented with `0-SFC`, defined as `gribName`. These values are used in the model definitions to pull out specific bands.

## models
Specify a new key in this object to create a new model definition.

### enabled
Whether the model is enabled or not. Setting to `false` will mean the model does not get processed.

### bounds
The bounds to clip the output of this model to, as defined in the `config` section.

### updateFrequency
How often the model is ran/updated. E.g. every 6 hours for ensembles or every hour for high resolution mesoscale models.

### updateOffset
For each day, which hour the model is first run for. Usually `0` (e.g. 00Z) for most models, but some models like the SREF start at 03Z.

### startTime
The first forecast hour of the model. Usually 0 or 1 depending on the model.

### endTime
The final forecast hour of the model (or the last one you want to process). Note that this determines the decimal format for the entire processing system. If you have a three digit `endTime` (such as 128) then the first forecast hour will be printed as `001`. Thus, unintended side effects will occur if you set the `endTime` to a two digit number like 72 even though the actual final forecst hour of the model has three digits.

### fhStep
Defines the number of hours / intervals between each step. Since many models have variable intervals, this is defined with the following format:
```
{
  "<hour the interval starts at>": <interval>
}
```
The first entry should match the `startTime`.

### fhStepManual
Use this instead of `fhStep` if the interval system cannot be handled by the above pattern. This may be the case with some models, such as `nbm`, which has sliding intervals due to its integration of hourly, 3-hourly, and 12-hourly updated models across an hourly-updating blend.

For this setting, you must manually define the `fhStep` for different arrays of initialization hours. For instance, the NBM requires six sets of `fhStep` definitions.

```
"fhStepManual": [
    {
        "appliesTo": [
            "00",
            "06",
            "12",
            "18"
        ],
        "fhStep": {
            "1": 1,
            "36": 3,
            "192": 6
        }
    },
    {
        "appliesTo": [
            "01",
            "07",
            "13",
            "19"
        ],
        "fhStep": {
            "1": 1,
            "35": 3,
            "191": 6
        }
    },
    {
        "appliesTo": [
            "02",
            "08",
            "14",
            "20"
        ],
        "fhStep": {
            "1": 1,
            "34": 3,
            "190": 6
        }
    },
    {
        "appliesTo": [
            "03",
            "09",
            "15",
            "21"
        ],
        "fhStep": {
            "1": 1,
            "33": 3,
            "189": 6
        }
    },
    {
        "appliesTo": [
            "04",
            "10",
            "16",
            "22"
        ],
        "fhStep": {
            "1": 1,
            "32": 3,
            "188": 6
        }
    },
    {
        "appliesTo": [
            "05",
            "11",
            "17",
            "23"
        ],
        "fhStep": {
            "1": 1,
            "31": 3,
            "187": 6
        }
    }
],
```

### url
The programmatic URL to pull the GRIB file from. You can use variable substitution:
 * `%D` - The date of the model run (e.g. 20190821)
 * `%H` - The hour of the model run (e.g. 12)
 * `%T` - The forecast hour (e.g. 34)

Example:
```
https://www.ftp.ncep.noaa.gov/data/nccf/com/gens/prod/gefs.%D/%H/pgrb2ap5/geavg.t%Hz.pgrb2a.0p50.f%T
```

### filetype
The filetype of the GRIB file. Almost always going to be `grib2`.

### index
Boolean, whether this model uses `.idx` files alongside the GRIB2 files. This allows for HTTP random access retrieval of individual bands, instead of pulling down the entire file.

### anl
Boolean, whether the first forecast hour of the model is called `anl` (analysis) or not. Some models seem to do this.

### flatTime
Boolean, true if the model doesn't use separate grib2 files for each forecast hour and instead just includes them all in one file as different bands. This is the case with SREF model outputs.

### customTranslate
GRIB2 files are not a primary focus of the GDAL library. Thus in the development of this API, multiple issues with the GRIB2 parser have been discovered and patched. These fixes may not be available in the widely distributed upstream versions of GDAL. Hopefully in the future this flag will be unnecessary. But for now, for some models, such as the NBM, only the latest, undistributed version of the GDAL source is capable of processing the files correctly.

If you've build gdal and python-gdal from the latest version of the source code, this may not be necessary. However if you are using a version from `aptitude` or `anaconda` these fixes are not available yet. `customTranslate` defines a shell command that will be run to convert the GRIB2 file to GeoTIFF before the script handles it. In this case, you can call a build of the gdal source, a docker image, or something like that to handle the GRIB2 with the latest gdal code.

This expects you provide some sort of path or shell command ending in a `gdal_translate` command. The necessary files and translate options are appended to what this setting is set to.

For instance, if you use the docker images included in the gdal library, you can just run the docker image containing the latest build of gdal to convert the grib2 file first before the script handles it:

```"customTranslate": "docker run --memory-swap='-1' --memory='4000m' --rm -v /home:/home newgdal gdal_translate"```

### customPathPrefixes
For the above `customTranslate`, lets you prepend a string to what file names the script uses for the `gdal_translate` command. For instance, with a docker image, you may want to prepend `$PWD/`. Note that even if you don't need a prefix you will still need to set an empty string if you are using `customTranslate`.

### bands
An array that defines the individual bands that will be pulled out of each model. A band definition looks like this:
```
{
    "var": <the name of the var as mentioned in the GRIB or .idx file> - such as CAPE, APCP, UGRD,
    "level": <the name of a level key, as defined in the levelMaps section of the config>
}
```

# Python Libs Required

 * psycopg2
 * python-dateutil
 * requests
 * osgeo (gdal, osr)
 * pytz

