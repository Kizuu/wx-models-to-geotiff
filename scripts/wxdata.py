import wxdata_lib.pg_connection_manager as pg
from wxdata_lib.config import config, levelMaps, models
import wxdata_lib.http_manager as http_manager
from wxdata_lib.logger import log, say_hello, print_line
import wxdata_lib.model_tools as model_tools
import wxdata_lib.processing as processing
import wxdata_lib.file_tools as file_tools

from datetime import datetime, timedelta
import os
import multiprocessing
import time
import random
import pytz
import copy
from osgeo import gdal
import pprint

agent_logged = False
first_run = True
processing_pool = {}
pp = pprint.PrettyPrinter(indent=4)
utc = pytz.UTC

gdal.UseExceptions()
tasks_last_updated = datetime.now()
processing_pool_updating = False


def kill_me(exit_code):
    if exit_code != 0:
        log("Exiting on failure.", "ERROR")

    if agent_logged:
        removed = pg.remove_agent()
        if not removed:
            log("Could not remove agent, trying again.", "ERROR", remote=True)
            try:
                pg.remove_agent()
            except:
                os._exit(exit_code)
    os._exit(exit_code)


def init():
    global agent_logged, processing_pool

    say_hello()
    if not pg.connect():
        kill_me(1)

    log("✓ Connected.", "DEBUG")

    if not pg.can_do_work():
        log("Another agent is running already. Goodbye.", "NOTICE")
        kill_me(0)

    agent_logged = pg.add_agent()
    if not agent_logged:
        kill_me(1)

    max_threads = config["maxThreads"]

    update_processing_pool()

    while len(get_open_tasks()) > 0:
        with multiprocessing.Pool(processes=max_threads) as pool:
            tasks = get_open_tasks()
            if len(tasks) == 0:
                break
            log("Open tasks: " + str(len(tasks)), "DEBUG", remote=True)

            for result in pool.imap_unordered(process, tasks, chunksize=1):
                code = result["code"]
                model_name = result["model_name"]
                step_name = result["step_name"]
                fh = result["fh"]
                timestamp = result["timestamp"]

                if code == "OK":
                    model_tools.update_last_fh(model_name, fh)
                    if model_name in processing_pool:
                        del processing_pool[model_name]["steps"][step_name]
                        if not bool(processing_pool[model_name]["steps"]) or ("flatTimeFullFile" in models[model_name] and models[model_name]["flatTimeFullFile"] == True):
                            del processing_pool[model_name]
                            model_tools.finish_model(model_name, timestamp)

                elif code == "PAUSE":
                    model_tools.set_as_paused(model_name, fh)
                    if model_name in processing_pool:
                        del processing_pool[model_name]

                elif code == "FAIL":
                    if model_name in processing_pool:
                        time.sleep(5)
                        step = processing_pool[model_name]["steps"][step_name]
                        step["retries"] += 1
                        if step["retries"] > config["maxRetriesPerStep"]:
                            log("Step " + model_name + ": " + step_name +
                                " failed permanently.", "ERROR", remote=True)
                            del processing_pool[model_name]["steps"][step_name]
                            if not bool(processing_pool[model_name]["steps"]):
                                del processing_pool[model_name]
                                model_tools.finish_model(model_name, timestamp)

                else:
                    if model_name in processing_pool:
                        del processing_pool[model_name]

    log("No more processing to do. Goodbye.", "NOTICE")
    time.sleep(1)
    kill_me(0)


def process(step):
    global processing_pool
    model_name = step["model_name"]
    timestamp = step["timestamp"]
    step_name = step["step_name"]
    pool_step = processing_pool[model_name]["steps"][step_name]
    if model_tools.get_model_status(model_name) == "PAUSED":
        log("Skipping paused model | " + model_name + " | " + step_name, "NOTICE")
        return {
            "code": "REMOVED",
            "fh": pool_step['fh'],
            "model_name": model_name,
            "step_name": step_name,
            "timestamp": timestamp
        }

    log("Starting processing | " + model_name + " | " + step_name, "INFO")
    res = processing.process(pool_step, model_name, timestamp)
    return {
        "code": res,
        "fh": pool_step['fh'],
        "model_name": model_name,
        "step_name": step_name,
        "timestamp": timestamp
    }


def get_open_tasks():
    if (datetime.now() - tasks_last_updated).total_seconds() > (config["pausedResumeMinutes"] * 60) and processing_pool_updating == False:
        log("Need to update the processing pool", "DEBUG")
        update_processing_pool()

    open_tasks = []
    new_processing_pool = copy.deepcopy(processing_pool)

    # Temporary(?) workaround - surpress nbm execution til
    # other models are done. Otherwise it makes everything wait
    # for its slow-ass steps to process.
    # Ideally, we should use something other than
    # pool.imap to feed steps into the multiprocessor
    model_choices = list(new_processing_pool.keys())
    # if len(model_choices) > 1 and 'nbm' in model_choices:
    #    del new_processing_pool['nbm']

    while bool(new_processing_pool):
        model_choices = list(new_processing_pool.keys())
        model_name = random.choice(model_choices)
        model = new_processing_pool[model_name]
        if "status" in model and model["status"] == "POPULATING":
            del new_processing_pool[model_name]
            continue

        timestamp = model["timestamp"]
        for step_name in model["steps"]:
            step = model["steps"][step_name]

        step_fh = -1
        for step_name in list(model["steps"]):
            step = model["steps"][step_name]
            fh = step['fh']
            if step_fh == -1 or step_fh == fh and step["processing"] == False:
                open_tasks.append({
                    'model_name': model_name,
                    'timestamp': timestamp,
                    'step': step,
                    'step_name': step_name
                })
                step_fh = fh
                del model["steps"][step_name]
            # Delete any steps later than the one to process
            # this keeps issues from occuring if the same
            # tif file is written to by two steps at the same time
            elif step_fh != -1 and step_fh != fh:
                del model["steps"][step_name]

        if len(model["steps"]) == 0:
            del new_processing_pool[model_name]

    return open_tasks


def update_processing_pool():
    global processing_pool, first_run, tasks_last_updated, processing_pool_updating
    log("Updating processing pool", "DEBUG")
    processing_pool_updating = True
    conn, curr = pg.ConnectionPool.connect()
    # Check only brand new models, or models that are waiting first
    for model_name, model in models.items():
        # Flag this model as disabled in the DB
        if not model["enabled"]:
            try:
                curr.execute(
                    "UPDATE wxdata.models SET status = %s WHERE model = %s", ("DISABLED", model_name))
                conn.commit()
            except Exception as e:
                log(repr(e), "ERROR", remote=True)
            continue

        if model_name in processing_pool:
            continue

        status = model_tools.get_model_status(model_name)
        if (status == "DISABLED"):
            status = "WAITING"

        model_fh = model_tools.get_full_fh(model_name, model["startTime"])
        log("Model: " + model_name, "INFO")

        lookback = 0

        if status == None:
            try:
                while lookback < config["maxLookback"]:
                    timestamp = model_tools.get_last_available_timestamp(
                        model, prev=lookback)
                    if model_tools.check_if_model_fh_available(model_name, timestamp, model_fh):
                        model_tools.add_model_to_db(model_name, timestamp)
                        init_new_run(processing_pool, model_name, timestamp)
                        model_tools.mark_model_as_processing(
                            model_name, timestamp)
                        break

                    lookback += 1
            except Exception as e:
                log(repr(e), "ERROR", remote=True)

        elif status == "WAITING":

            log("Status: " + status, "INFO")

            prev_timestamp = model_tools.get_model_timestamp(
                model_name).replace(tzinfo=utc)

            if prev_timestamp == None:
                log("Couldn't get previous timestamp, continuing.",
                    "WARN", remote=True)
                continue

            log("Prev timestamp: " + str(prev_timestamp), "INFO")
            while lookback < config["maxLookback"]:
                try:
                    timestamp = model_tools.get_last_available_timestamp(
                        model, prev=lookback)

                    if timestamp <= prev_timestamp:
                        log("· No newer runs exist.", "INFO", indentLevel=1)
                        break

                    if model_tools.check_if_model_fh_available(model_name, timestamp, model_fh):
                        init_new_run(processing_pool, model_name, timestamp)
                        model_tools.mark_model_as_processing(
                            model_name, timestamp)
                        break

                    else:
                        log("· Nope.", "INFO", indentLevel=1)

                    lookback += 1

                except Exception as e:
                    log(repr(e), "ERROR", remote=True)

        elif status == "PAUSED":

            try:
                curr.execute(
                    "SELECT paused_at FROM wxdata.models WHERE model LIKE '" + model_name + "'"
                )
                paused_at = curr.fetchone()[0]

                log(model_name + " is PAUSED.", "INFO")

                if abs(datetime.now().replace(tzinfo=utc) - paused_at.replace(tzinfo=utc)) >= timedelta(minutes=config["pausedResumeMinutes"]):
                    log("Restarting paused model " + model_name, "NOTICE")
                    timestamp = model_tools.get_model_timestamp(
                        model_name).replace(tzinfo=utc)
                    get_non_complete_processing_pool(
                        processing_pool, model_name, curr
                    )
                    model_tools.mark_model_as_processing(model_name, timestamp)
                else:
                    log("Not resuming yet until the threshold of " +
                        str(config["pausedResumeMinutes"]) + " minutes is met.", "INFO")

            except Exception as e:
                log("Error in pause resumption -- " +
                    repr(e), "ERROR", remote=True)

        # This shouldn't be necessary, but it will resume models that were in
        # process if the script were to die
        elif status == "PROCESSING" and model_name not in processing_pool:
            log("Resurrecting dead model " + model_name, "NOTICE", remote=True)
            try:
                timestamp = model_tools.get_model_timestamp(
                    model_name).replace(tzinfo=utc)
                get_non_complete_processing_pool(
                    processing_pool, model_name, curr
                )
                model_tools.mark_model_as_processing(model_name, timestamp)
            except Exception as e:
                log(repr(e), "ERROR", remote=True)

        elif status == "ERROR":
            log("Couldn't retrieve the status for some reason.", "WARN")

    pg.ConnectionPool.close(conn, curr)
    tasks_last_updated = datetime.now()
    processing_pool_updating = False
    log("Done updating the model pool.", "DEBUG")


def init_new_run(processing_pool, model_name, timestamp):
    processing_pool[model_name] = {
        'timestamp': timestamp,
        'status': 'POPULATING',
        'steps': {}
    }
    log(f"Initializing new run for {model_name} | {timestamp}.",
        "NOTICE", indentLevel=0, remote=True, model=model_name)
    processing_pool[model_name]["steps"] = model_tools.make_band_dict(
        model_name, timestamp.strftime("%H"))
    processing_pool[model_name]["status"] = "READY"


def get_non_complete_processing_pool(processing_pool, model_name, curr):

    last_fh = 0
    curr.execute(
        "SELECT lastfh, timestamp FROM wxdata.models WHERE model = %s", (model_name,))
    result = curr.fetchone()
    last_fh = int(result[0])
    timestamp = result[1]

    processing_pool[model_name] = {
        'timestamp': timestamp,
        'status': 'POPULATING',
        'steps': {}
    }
    processing_pool[model_name]["steps"] = model_tools.make_band_dict(
        model_name, timestamp.strftime("%H")
    )
    for step in list(processing_pool[model_name]['steps']):
        step_fh = processing_pool[model_name]['steps'][step]['fh']
        if int(step_fh) < last_fh:
            del processing_pool[model_name]['steps'][step]
    processing_pool[model_name]["status"] = "READY"


if __name__ == "__main__":
    init()
