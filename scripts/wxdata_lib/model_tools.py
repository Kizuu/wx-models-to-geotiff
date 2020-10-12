from .config import config, models, levelMaps
from .logger import log
from . import file_tools as file_tools
from . import pg_connection_manager as pg

from datetime import datetime, timedelta, tzinfo, time
import requests
import pytz
import signal

utc = pytz.UTC


class TimeoutException(Exception):
    pass


def timeout_handler(signum, frame):
    raise TimeoutException


def make_url(model_name, model_date, model_hour, fh):
    model = models[model_name]
    return model["url"].replace("%D", model_date).replace("%H", model_hour).replace("%T", fh)


def add_model_to_db(model_name, timestamp):
    try:
        log("✓ Added model to models table.", "INFO",
            indentLevel=1, remote=True, model=model_name)
        conn, curr = pg.ConnectionPool.connect()
        curr.execute(
            "INSERT INTO wxdata.models (model, status, timestamp) VALUES (%s, %s, %s)", (model_name, "WAITING", timestamp))
        conn.commit()
        pg.ConnectionPool.close(conn, curr)
        return True
    except:
        pg.ConnectionPool.close(conn, curr)
        log("Couldn't add model to db.", "ERROR", remote=True, model=model_name)
        return False


def get_last_available_timestamp(model, prev=0):
    try:
        now = datetime.utcnow()
        start_of_day = now - timedelta(
            hours=now.hour,
            minutes=now.minute,
            seconds=now.second,
            microseconds=now.microsecond
        )
        yesterday_midnight = start_of_day - timedelta(days=1)

        start_of_day_checker = yesterday_midnight + \
            timedelta(hours=model["updateOffset"])

        max_time = now - timedelta(hours=prev*model["updateFrequency"])
        now_not_exceeded = True
        checked_time = start_of_day_checker
        while now_not_exceeded:
            if checked_time + timedelta(hours=(model["updateFrequency"])) > max_time:
                break
            else:
                checked_time = checked_time + \
                    timedelta(hours=(model["updateFrequency"]))

        log(f"· Last available timestamp, {str(prev)} runs ago: {str(checked_time)}",
            "DEBUG", indentLevel=1)

        return checked_time.replace(tzinfo=utc)

    except Exception as e:
        log(repr(e), "ERROR")


def get_number_of_hours(model_name, hour):
    model = models[model_name]
    fh = model["startTime"]
    i = 0
    while True:
        fh = add_appropriate_fh_step(model_name, fh, hour)
        i += 1

        if fh > model["endTime"]:
            return i


def get_model_status(model_name):
    try:
        conn, curr = pg.ConnectionPool.connect()
        curr.execute(
            "SELECT status FROM wxdata.models WHERE model LIKE '" + model_name + "'")

        if curr.rowcount == 0:
            return None
        result = curr.fetchone()
        pg.ConnectionPool.close(conn, curr)
        return result[0]

    except Exception as e:
        pg.ConnectionPool.close(conn, curr)
        log(repr(e), "ERROR")
        return "ERROR"


def get_model_timestamp(model_name):
    try:
        conn, curr = pg.ConnectionPool.connect()
        curr.execute(
            "SELECT timestamp FROM wxdata.models WHERE model LIKE '" + model_name + "'")
        result = curr.fetchone()
        pg.ConnectionPool.close(conn, curr)
        return result[0]

    except:
        pg.ConnectionPool.close(conn, curr)
        return None


def get_full_fh(model_name, fh):
    model = models[model_name]
    return str(fh).rjust(len(str(model["endTime"])), '0')


def get_level_name_for_level(level_shorthand, name_type):
    return levelMaps[level_shorthand][name_type]


def check_if_model_fh_available(model_name, timestamp, fh):
    url = make_url(model_name, timestamp.strftime(
        "%Y%m%d"), timestamp.strftime("%H"), fh)
    log("· Checking URL: " + url, "DEBUG",
        remote=True, indentLevel=1, model=model_name)

    try:
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(10)
        try:
            ret = requests.head(url, timeout=(3, 5))
        except TimeoutException:
            raise Exception("Timeout alarm tripped")
        except:
            raise
        finally:
            signal.alarm(0)

        if ret.status_code >= 200 and ret.status_code < 300:
            log("✓ Found.", "DEBUG", remote=True,
                indentLevel=1, model=model_name)
            return True
        else:
            if ret.status_code == None:
                log("× Status code for fh was None. " + url, "WARN", remote=True,
                    indentLevel=1, model=model_name)
            else:
                log("× Not found -- Status code " + str(ret.status_code), "INFO", remote=True,
                    indentLevel=1, model=model_name)

    except Exception as e:
        log("× Not found -- Exception.", "DEBUG", remote=True,
            indentLevel=1, model=model_name)
        log(repr(e), "ERROR", indentLevel=1, remote=True)
    finally:
        signal.alarm(0)

    return False


def model_timestamp_matches(model_name, timestamp):
    try:
        conn, curr = pg.ConnectionPool.connect()
        curr.execute(
            "SELECT timestamp FROM wxdata.models WHERE model = %s", (model_name,))
        model_time = str(curr.fetchone()[0])[0:16]
        t_time = str(timestamp)[0:16]
        pg.ConnectionPool.close(conn, curr)
        return model_time == t_time
    except:
        pg.ConnectionPool.close(conn, curr)
        return False


def get_base_filename(model_name, timestamp, var_level):
    date = timestamp.strftime("%Y%m%d")
    time = timestamp.strftime("%HZ")
    file = model_name + "_" + date + "_" + time
    if var_level is not None:
        return file + "_" + var_level
    else:
        return file


# This iterates a fh by the appropriate step size,
# given the fh. This is for models where the fh step size
# increases after a certain hour.
def add_appropriate_fh_step(model_name, fh, currentHr="00"):

    fhSteps = {}

    model = models[model_name]
    if "fhStepManual" in model:
        manualDef = model["fhStepManual"]
        for fhDef in manualDef:
            for hour in fhDef["appliesTo"]:
                if hour == currentHr:
                    fhSteps = fhDef["fhStep"]
                    break

            if bool(fhSteps):
                break

    else:
        fhSteps = model["fhStep"]

    for key in reversed(sorted(fhSteps.keys(), key=lambda x: float(x))):
        if fh >= int(key):
            return fh + fhSteps[key]

    log("× Couldn't match the appropriate step size.",
        "WARN", indentLevel=1, remote=True, model=model_name)
    return fh


def make_band_dict(model_name, hour):
    log(f"· Creating band dict.", "DEBUG",
        indentLevel=1, remote=True, model=model_name)

    band_dict = {}

    model = models[model_name]

    fh = model["startTime"]
    i = 1

    bands = make_model_band_array(model_name)

    while True:
        full_fh = get_full_fh(model_name, fh)
        if bands == None or len(bands) == 0:
            band_dict.update({
                full_fh: {
                    'retries': 0,
                    'fh': full_fh,
                    'band_num': i,
                    'processing': False
                }
            })
        else:
            for band in bands:

                band_copy = band.copy()

                vals = {
                    'retries': 0,
                    'fh': full_fh,
                    'band_num': i,
                    'band': band_copy,
                    'processing': False
                }

                if ("flatTime" in model and model["flatTime"]) or ("flatTimeFullFile" in model and model["flatTimeFullFile"]):
                    time_val = fh
                    if "anl" in model and fh == model["startTime"]:
                        time_val = "anl"

                    band_copy["time"] = str(time_val)

                band_dict.update({
                    band["shorthand"] + "_" + full_fh: vals
                })
        fh = add_appropriate_fh_step(model_name, fh, hour)
        i += 1

        if fh > model["endTime"]:
            log(f"Band dict created.", "NOTICE",
                indentLevel=0, remote=True, model=model_name)

            return band_dict


def make_model_band_array(model_name, force=False):
    model = models[model_name]
    if not "bands" in model:
        return None

    model_band_array = []
    if model["index"] or force:
        for band in model["bands"]:
            shorthand = band["var"].lower() + "_" + band["level"].lower()
            if "output" in band:
                shorthand += "_" + band["output"]
            model_band_array.append({
                "shorthand": shorthand,
                "band": band
            })

    return model_band_array


def set_as_paused(model_name, full_fh):
    conn, curr = pg.ConnectionPool.connect()
    try:
        curr.execute("UPDATE wxdata.models SET (status,lastfh,paused_at) = (%s, %s, %s) WHERE model = %s",
                     ("PAUSED", full_fh, datetime.now().isoformat(), model_name))
        conn.commit()
    except:
        log("Couldn't set " + model_name + " to paused.", "ERROR", remote=True)
    finally:
        pg.ConnectionPool.close(conn, curr)


def update_last_fh(model_name, full_fh):
    conn, curr = pg.ConnectionPool.connect()
    try:
        curr.execute("UPDATE wxdata.models SET lastfh = %s WHERE model = %s",
                     (full_fh, model_name))
        conn.commit()
    except Exception as e:
        print(repr(e))
        log("Couldn't update model's last fh " +
            model_name, "ERROR", remote=True)
    finally:
        pg.ConnectionPool.close(conn, curr)


def mark_model_as_complete(model_name, timestamp):
    conn, curr = pg.ConnectionPool.connect()
    try:
        curr.execute(
            "UPDATE wxdata.models SET status = %s WHERE model = %s", ("WAITING", model_name))
        conn.commit()

        curr.execute(
            "UPDATE wxdata.run_status SET status = 'COMPLETE' WHERE model = %s AND timestamp = %s", (model_name, timestamp))
        conn.commit()
    except Exception as e:
        print(repr(e))
        log("Couldn't update model to waiting " +
            model_name, "ERROR", remote=True)
    finally:
        pg.ConnectionPool.close(conn, curr)


def mark_model_as_processing(model_name, timestamp):
    conn, curr = pg.ConnectionPool.connect()
    formatted_timestamp = timestamp.strftime('%Y%m%d_%HZ')
    try:
        log(f"· Start processing {model_name} | {formatted_timestamp}.",
            "INFO", indentLevel=1, remote=True, model=model_name)

        curr.execute("UPDATE wxdata.models SET (status, timestamp) = (%s, %s) WHERE model = %s",
                     ("PROCESSING", timestamp, model_name))
        conn.commit()

        curr.execute(
            "DELETE FROM wxdata.run_status WHERE model = %s AND timestamp = %s", (model_name, timestamp))
        conn.commit()

        curr.execute("INSERT INTO wxdata.run_status (model, status, timestamp) VALUES (%s, %s, %s)",
                     (model_name, "PROCESSING", timestamp))
        conn.commit()
    except:
        log("Could not set the model status back to processing! This requires manual intervention.",
            "ERROR", remote=True)
    finally:
        pg.ConnectionPool.close(conn, curr)


def finish_model(model_name, timestamp):
    log(model_name + " is completely finished processing.",
        "NOTICE", remote=True)
    mark_model_as_complete(model_name, timestamp)
    file_tools.clean()
    pg.clean()
