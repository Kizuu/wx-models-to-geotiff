from .config import config, models
from .logger import log

from . import model_tools as model_tools
from . import pg_connection_manager as pg
from .http_manager import http
import subprocess
import sys

from datetime import datetime, timedelta, tzinfo, time
import os
import random
import requests
import signal

from osgeo import ogr, gdal, osr, gdalconst


class TimeoutException(Exception):
    pass


def timeout_handler(signum, frame):
    raise TimeoutException


def process(step, model_name, timestamp):

    log("· Trying to process a step in model " + model_name, "INFO")
    full_fh = step['fh']
    band_num = step['band_num']

    log("Preparing to process " + model_name + " | fh: " + full_fh, "INFO")

    band = None
    band_info_str = ' | (no var/level)'
    if 'band' in step:
        band = step['band']
        band_info_str = ' | band ' + band['shorthand']

    file_exists = model_tools.check_if_model_fh_available(
        model_name, timestamp, full_fh)

    if not file_exists:
        log("Remote data not ready yet. " + model_name + " | fh: " +
            full_fh + band_info_str, 'NOTICE', remote=True, model=model_name)
        return 'PAUSE'

    log("Processing for " + model_name + " | fh: " + full_fh +
        band_info_str, "NOTICE", remote=True, model=model_name)

    try:
        if band is None:
            if not download_full_file(model_name, timestamp, full_fh, band_num):
                return 'FAIL'
        else:
            if not download_band(model_name, timestamp, full_fh, band, band_num):
                return 'FAIL'

        log("Successfully processed " + model_name +
            " | fh: " + full_fh + band_info_str, "NOTICE")

        return 'OK'

    except Exception as e:
        log("Failure.", "ERROR", remote=True)
        log(repr(e), "ERROR", indentLevel=2, remote=True, model=model_name)
        return 'FAIL'


'''
    Uses an .idx file to download an individual band and convert it to a TIF library.
'''


def download_band(model_name, timestamp, fh, band, band_num):
    model = models[model_name]

    url = model_tools.make_url(model_name, timestamp.strftime(
        "%Y%m%d"), timestamp.strftime("%H"), fh)

    file_name = model_tools.get_base_filename(
        model_name, timestamp, band["shorthand"])
    target_dir = config["mapfileDir"] + "/" + model_name + "/"
    download_filename = config["tempDir"] + "/" + \
        file_name + "_t" + fh + "." + model["filetype"]
    target_filename = target_dir + file_name + ".tif"

    try:
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(10)
        try:
            response = requests.head(url, timeout=(10, 30))
        except TimeoutException:
            raise Exception("Timeout alarm tripped")
        except:
            raise
        else:
            signal.alarm(0)
        if response.status_code != 200 or response.status_code == None or response == None:
            log(f"· This index file is not ready yet. " + url,
                "WARN", remote=True, indentLevel=2, model=model_name)
            return False

        content_length = str(response.headers["Content-Length"])
    except Exception as e:
        log(f"· Couldn't get header of " + url, "ERROR",
            remote=True, indentLevel=2, model=model_name)
        log(repr(e), "ERROR")
        return False

    byte_range = get_byte_range(band, url + ".idx", content_length)

    if not byte_range or byte_range == None:
        log(f"· Band {band['shorthand']} doesn't exist for fh {fh}.",
            "WARN", remote=True, indentLevel=2, model=model_name)
        return False

    log(f"↓ Downloading band {band['shorthand']} for fh {fh}.",
        "INFO", indentLevel=2, remote=True, model=model_name)
    try:
        response = http.request('GET', url,
                                headers={
                                    'Range': 'bytes=' + byte_range
                                },
                                retries=5)

        with open(download_filename, 'wb') as f:
            f.write(response.data)

    except Exception as e:
        log("Couldn't read the band -- the request likely timed out. " +
            fh, "ERROR", indentLevel=2, remote=True, model=model_name)

        log(repr(e), "ERROR", remote=True, model=model_name)
        return False

    log(f"✓ Downloaded band {band['shorthand']} for fh {fh}.",
        "INFO", indentLevel=2, remote=True, model=model_name)

    bounds = config["bounds"][model["bounds"]]
    epsg4326 = osr.SpatialReference()
    epsg4326.ImportFromEPSG(4326)

    log("· Warping downloaded data.", "INFO",
        indentLevel=2, remote=True, model=model_name)
    try:
        if "customTranslate" in model:

            p = subprocess.run(
                model["customTranslate"] + [
                    model["customPathPrefix"] + download_filename,
                    model["customPathPrefix"] +
                    download_filename + "_staged.tif",
                    "-co", "interleave=band", "-co", "bigtiff=yes"],
                close_fds=True,
                timeout=3600,
                bufsize=-1
            )

            grib_file = gdal.Open(download_filename + "_staged.tif")

        else:
            grib_file = gdal.Open(download_filename)

        out_file = gdal.Warp(
            download_filename + ".tif",
            grib_file,
            format='GTiff',
            outputBounds=[bounds["left"], bounds["bottom"],
                          bounds["right"], bounds["top"]],
            dstSRS=epsg4326,
            creationOptions=["BIGTIFF=YES", "INTERLEAVE=BAND"],
            resampleAlg=gdal.GRA_CubicSpline)
        out_file.FlushCache()
        out_file = None

        grib_file = None
    except subprocess.CalledProcessError as e:
        log("Custom function failed with " + str(e.returncode),
            "ERROR", remote=True, model=model_name)
        log(e.output, "ERROR", remote=True, model=model_name)
    except Exception as e:
        log("Warping failed -- " + download_filename, "ERROR", remote=True)
        log(repr(e), "ERROR", indentLevel=2, remote=True, model=model_name)
        return False

    # check to see if the working raster exists
    if not os.path.exists(target_filename):
        log(f"· Creating output master TIF | {target_filename}",
            "INFO", indentLevel=2, remote=True, model=model_name)
        try:
            os.makedirs(target_dir)
        except:
            log("· Directory already exists.", "INFO",
                indentLevel=2, remote=False, model=model_name)

        num_bands = model_tools.get_number_of_hours(
            model_name, timestamp.strftime("%H"))

        try:
            grib_file = gdal.Open(download_filename + ".tif")
            geo_transform = grib_file.GetGeoTransform()
            width = grib_file.RasterXSize
            height = grib_file.RasterYSize

            new_raster = gdal.GetDriverByName('MEM').Create(
                '', width, height, num_bands, gdal.GDT_Float32)
            new_raster.SetProjection(grib_file.GetProjection())
            new_raster.SetGeoTransform(list(geo_transform))
            gdal.GetDriverByName('GTiff').CreateCopy(
                target_filename, new_raster, 0)
            log("✓ Output master TIF created --> " + target_filename, "NOTICE",
                indentLevel=1, remote=True, model=model_name)
            new_raster = None
            grib_file = None
        except Exception as e:
            log("Couldn't create the new master TIF: " + target_filename,
                "ERROR", indentLevel=1, remote=True, model=model_name)
            log(repr(e), "ERROR", indentLevel=2, remote=True, model=model_name)
            return False

    log(f"· Writing data to the GTiff | band: {band['shorthand']} | fh: {fh} | band_number: {str(band_num)}",
        "INFO", indentLevel=2, remote=True, model=model_name)

    sub_band_num = 1
    if "subBandNum" in band["band"]:
        sub_band_num = band["band"]["subBandNum"]

    try:
        # Copy the downloaded band to this temp file
        grib_file = gdal.Open(download_filename + ".tif")
        data = grib_file.GetRasterBand(sub_band_num).ReadAsArray()

        tif = gdal.Open(target_filename, gdalconst.GA_Update)
        tif.GetRasterBand(band_num).WriteArray(data)
        tif.FlushCache()

        grib_file = None
        tif = None
        data = None
        log(f"✓ Data written to the GTiff | band: {band['shorthand']} | fh: {fh}.",
            "INFO", indentLevel=2, remote=True, model=model_name)
    except Exception as e:
        log(f"Couldn't write band to TIF | band: {band['shorthand']} | fh: {fh}.",
            "ERROR", indentLevel=2, remote=True, model=model_name)
        log(repr(e), "ERROR", indentLevel=2, remote=True, model=model_name)
        return False

    try:
        os.remove(download_filename)
    except:
        log(f"× Could not delete a temp file ({download_filename}).",
            "DEBUG", indentLevel=2, remote=True, model=model_name)
    try:
        os.remove(download_filename + ".tif")
    except:
        log(f"× Could not delete a temp file ({download_filename + '.tif'}).",
            "DEBUG", indentLevel=2, remote=True, model=model_name)
    try:
        os.remove(download_filename + "_staged.tif")
    except:
        log(f"× Could not delete a temp file ({download_filename + '_staged.tif'}).",
            "DEBUG", indentLevel=2, remote=True, model=model_name)

    try:
        os.remove(download_filename + ".tif.aux.xml")
    except:
        log(f"× Could not delete a temp file ({download_filename + '.tif.aux.xml'}).",
            "DEBUG", indentLevel=2, remote=True, model=model_name)
    try:
        os.remove(download_filename + "_staged.tif.aux.xml")
    except:
        log(f"× Could not delete a temp file ({download_filename + '_staged.tif.aux.xml'}).",
            "DEBUG", indentLevel=2, remote=True, model=model_name)

    return True


'''
    Downloads a full GRIB2 file for a timestamp, then extracts each var/level
    to convert to separate TIF libraries.
'''


def download_full_file(model_name, timestamp, fh, band_num):
    model = models[model_name]

    url = model_tools.make_url(model_name, timestamp.strftime(
        "%Y%m%d"), timestamp.strftime("%H"), fh)

    file_name = model_tools.get_base_filename(model_name, timestamp, None)
    target_dir = config["mapfileDir"] + "/" + model_name + "/"
    download_filename = config["tempDir"] + "/" + \
        file_name + "_t" + fh + "." + model["filetype"]

    try:
        os.makedirs(target_dir)
    except:
        log("· Directory already exists.", "INFO",
            indentLevel=2, remote=False, model=model_name)

    log(f"↓ Downloading fh {fh}.", "INFO",
        indentLevel=2, remote=True, model=model_name)
    try:
        response = http.request('GET', url, retries=5)
        log("Url: " + url, "DEBUG", indentLevel=2)
        log("Download: " + download_filename, "DEBUG", indentLevel=2)

        with open(download_filename, 'wb') as f:
            f.write(response.data)

        del response

        log(f"✓ Downloaded band fh {fh}.", "INFO",
            indentLevel=2, remote=True, model=model_name)
    except Exception as e:
        log("Couldn't read the fh -- the request likely timed out. " +
            fh, "ERROR", indentLevel=2, remote=True, model=model_name)
        log(repr(e), "ERROR", indentLevel=2, remote=True, model=model_name)
        return False

    bounds = config["bounds"][model["bounds"]]

    try:
        epsg4326 = osr.SpatialReference()
        epsg4326.ImportFromEPSG(4326)

        log("· Warping downloaded data.", "INFO",
            indentLevel=2, remote=True, model=model_name)
        try:
            os.remove(download_filename + ".tif")
        except:
            log("· No old file to remove.", "DEBUG", indentLevel=2)

        if "customTranslate" in model:
            p = subprocess.run(
                model["customTranslate"] + [
                    model["customPathPrefix"] + download_filename,
                    model["customPathPrefix"] +
                    download_filename + "_staged.tif",
                    "-co", "interleave=band", "-co", "bigtiff=yes"],
                close_fds=True,
                timeout=3600,
                bufsize=-1
            )
            filename = download_filename + "_staged.tif"
        else:
            filename = download_filename

        log
        grib_file = gdal.Open(filename)

        new_file = gdal.Warp(
            download_filename + ".tif",
            grib_file,
            format='GTiff',
            outputBounds=[bounds["left"], bounds["bottom"],
                          bounds["right"], bounds["top"]],
            dstSRS=epsg4326,
            creationOptions=["BIGTIFF=YES", "INTERLEAVE=BAND"],
            resampleAlg=gdal.GRA_CubicSpline)

        del new_file
        del grib_file
        del epsg4326

    except Exception as e:
        log("Warping failed -- " + download_filename, "ERROR",
            indentLevel=2, remote=True, model=model_name)
        log(repr(e), "ERROR", indentLevel=2, remote=True, model=model_name)
        return False

    num_bands = model_tools.get_number_of_hours(
        model_name, timestamp.strftime("%H"))

    bands = model_tools.make_model_band_array(model_name, force=True)

    log(f"· Extracting bands for fh {fh}.", "INFO",
        indentLevel=2, remote=True, model=model_name)

    for band in bands:
        target_filename = target_dir + \
            model_tools.get_base_filename(
                model_name, timestamp, band["shorthand"]) + ".tif"
        if not os.path.exists(target_filename):
            log(f"· Creating output master TIF with {str(num_bands) } bands | {target_filename}",
                "INFO", indentLevel=2, remote=True, model=model_name)
            try:
                os.makedirs(target_dir)
            except:
                log("· Directory already exists.", "INFO",
                    indentLevel=2, remote=True, model=model_name)

            try:
                grib_file = gdal.Open(download_filename + ".tif")
                geo_transform = grib_file.GetGeoTransform()
                width = grib_file.RasterXSize
                height = grib_file.RasterYSize

                new_raster = gdal.GetDriverByName('MEM').Create(
                    '', width, height, num_bands, gdal.GDT_Float32)
                new_raster.SetProjection(grib_file.GetProjection())
                new_raster.SetGeoTransform(list(geo_transform))
                gdal.GetDriverByName('GTiff').CreateCopy(
                    target_filename, new_raster, 0)
                grib_file = None
                new_raster = None
                log("✓ Output master TIF created. --> " + target_filename, "NOTICE",
                    indentLevel=1, remote=True, model=model_name)
            except:
                log("Couldn't create the new master TIF. --> " + target_filename, "ERROR",
                    indentLevel=1, remote=True, model=model_name)
                return False

        log(f"· Writing data to the GTiff | band: {band['shorthand']} | fh: {fh}",
            "INFO", indentLevel=2, remote=True, model=model_name)
        # Copy the downloaded band to this temp file
        try:
            grib_file = gdal.Open(download_filename + ".tif")
            gribnum_bands = grib_file.RasterCount
            band_level = model_tools.get_level_name_for_level(
                band["band"]["level"], "gribName")
            tif = gdal.Open(target_filename, gdalconst.GA_Update)
            for i in range(1, gribnum_bands + 1):
                try:
                    file_band = grib_file.GetRasterBand(i)
                    metadata = file_band.GetMetadata()
                    if ((
                            "ignoreBandVar" in model and
                            model["ignoreBandVar"] == True
                        ) or (
                            metadata["GRIB_ELEMENT"].lower() == band["band"]["var"].lower() and
                            metadata["GRIB_SHORT_NAME"].lower() == band_level.lower() and (
                                "comment" not in band["band"] or
                                band["band"]["comment"].lower(
                                ) == metadata["GRIB_COMMENT"].lower()
                            )
                    )):
                        log("· Band " + band["band"]["var"] + " found.",
                            "DEBUG", indentLevel=2, remote=False)
                        data = file_band.ReadAsArray()
                        if "flatTimeFullFile" in model and model["flatTimeFullFile"] == True:
                            tif.GetRasterBand(i).WriteArray(data)
                        else:
                            tif.GetRasterBand(band_num).WriteArray(data)
                            break

                except Exception as e:
                    log(f"× Couldn't read GTiff band: #{str(i)} | fh: {fh}",
                        "WARN", indentLevel=2, remote=True, model=model_name)
                    log(repr(e), "ERROR")

            tif.FlushCache()

            grib_file = None
            tif = None
            data = None
            file_band = None
        except Exception as e:
            return False

    try:
        os.remove(download_filename)
    except:
        log(f"× Could not delete a temp file ({download_filename}).",
            "DEBUG", indentLevel=2, remote=True, model=model_name)
    try:
        os.remove(download_filename + ".tif")
    except:
        log(f"× Could not delete a temp file ({download_filename + '.tif'}).",
            "DEBUG", indentLevel=2, remote=True, model=model_name)
    try:
        os.remove(download_filename + "_staged.tif")
    except:
        log(f"× Could not delete a temp file ({download_filename + '_staged.tif'}).",
            "DEBUG", indentLevel=2, remote=True, model=model_name)
    try:
        os.remove(download_filename + ".tif.aux.xml")
    except:
        log(f"× Could not delete a temp file ({download_filename + '.tif.aux.xml'}).",
            "DEBUG", indentLevel=2, remote=True, model=model_name)
    try:
        os.remove(download_filename + "_staged.tif.aux.xml")
    except:
        log(f"× Could not delete a temp file ({download_filename + '_staged.tif.aux.xml'}).",
            "DEBUG", indentLevel=2, remote=True, model=model_name)
    return True


'''
    Copied a bit from https://github.com/cacraig/grib-inventory/ - thanks!
'''


def get_byte_range(band, idx_file, content_length):
    log(f"· Searching for band defs in index file {idx_file}",
        "DEBUG", indentLevel=2, remote=True)
    try:
        response = http.request('GET', idx_file)
        data = response.data.decode('utf-8')
        var_name_to_find = band["band"]["var"]

        if "idxVar" in band["band"]:
            var_name_to_find = band["band"]["idxVar"]

        level_to_find = model_tools.get_level_name_for_level(
            band["band"]["level"], "idxName")

        found = False
        start_byte = None
        end_byte = None
        skipped_for_subband = False

        for line in data.splitlines():
            line = str(line)
            parts = line.split(':')
            var_name = parts[3]
            level = parts[4]
            time = parts[5]

            if found:
                if "subBandNum" in band["band"] and band["band"]["subBandNum"] and not skipped_for_subband:
                    skipped_for_subband = True
                else:
                    end_byte = parts[1]
                    break

            if var_name == var_name_to_find and level == level_to_find:
                if "hourRange" in band["band"]:
                    range_val = time.split(" ", 1)[0]
                    ranges = range_val.split("-")
                    if (int(ranges[1]) - int(ranges[0])) != band["band"]["hourRange"] or "day" in time:
                        continue

                if "time" in band and "day" not in time:
                    hr = time.split(" ", 1)[0]
                    if "-" in hr:
                        hr = hr.split("-")[1]

                    if hr != band["time"]:
                        continue

                if "comment" in band["band"]:
                    if len(parts) <= 6:
                        continue

                    if band["band"]["comment"] != parts[6]:
                        continue

                log("✓ Found.", "DEBUG", indentLevel=2, remote=False)

                found = True
                start_byte = parts[1]
                continue

        if found:
            if end_byte == None:
                end_byte = content_length

            log(f"· Bytes {start_byte} to {end_byte}", "DEBUG", indentLevel=2)
            if start_byte == end_byte:
                return None

            return start_byte + "-" + end_byte
        else:
            log(f"· Couldn't find band def in index file.",
                "WARN", indentLevel=2, remote=True)
        return None

    except Exception as e:
        log(f"Band def retrieval failed.", "ERROR", indentLevel=2, remote=True)
        log(repr(e), "ERROR")
        return None
