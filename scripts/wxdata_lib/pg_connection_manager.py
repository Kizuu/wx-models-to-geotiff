from .logger import log
from .config import config, models, levelMaps
from datetime import datetime
import psycopg2
from psycopg2 import pool
import os

pid = str(os.getpid())


'''
    This used to be a nice singleton that managed a threaded connection pool.
    So you wouldn't have to re-connect to run queries and whatnot.
    But since I moved to multiprocessing, this fails with SSL errors
    due to attempting to copy the connection between processes.

    So now it's just a shell of itself.
    Maybe somebody smarter than me can figure out a better way of managing
    a single psql connection between multiple processes.
'''


class ConnectionPool:
    __instance = None

    @staticmethod
    def connect():
        conn = psycopg2.connect(host=config["postgres"]["host"],
                                port=5432,
                                dbname=config["postgres"]["db"],
                                user=config["postgres"]["user"],
                                sslmode="require")

        curr = conn.cursor()
        return conn, curr

    @staticmethod
    def close(conn, curr):
        try:
            curr.close()
            conn.close()
        except:
            log("Couldn't close pool", "DEBUG")

    def __init__(self):
        if ConnectionPool.__instance == None:
            ConnectionPool.__instance = self

        #log("Making new pg connection pool...", "NOTICE")


def add_agent():
    try:
        conn, curr = ConnectionPool.connect()
        curr.execute(
            "INSERT INTO wxdata.agents (pid, start_time) VALUES (%s, %s)", (pid, datetime.utcnow()))
        conn.commit()
        ConnectionPool.close(conn, curr)
    except Exception as e:
        ConnectionPool.close(conn, curr)
        log("Couldn't add agent.", "ERROR")
        log(repr(e), "ERROR", indentLevel=1, remote=True)
        return False
    return True


def remove_agent():
    log("Removing agent " + pid, "DEBUG")
    try:
        conn, curr = ConnectionPool.connect()
        curr.execute(
            "DELETE FROM wxdata.agents WHERE pid = %s", (pid,))
        conn.commit()
        ConnectionPool.close(conn, curr)
    except Exception as e:
        print(repr(e))
        ConnectionPool.close(conn, curr)
        log("Couldn't remove agent.", "ERROR", remote=True)
        return False
    return True


def can_do_work():
    try:
        conn, curr = ConnectionPool.connect()
        curr.execute("SELECT COUNT(*) FROM wxdata.agents")
        conn.commit()
        result = curr.fetchone()
        if result[0] == 0:
            ConnectionPool.close(conn, curr)
            return True

        curr.execute("SELECT * FROM wxdata.agents")
        conn.commit()
        result = curr.fetchone()

        if os.path.exists("/proc/" + result[0]):
            ConnectionPool.close(conn, curr)
            return False
        else:
            log("An agent exists, but is not running.", "WARN", remote=True)
            curr.execute("DELETE FROM wxdata.agents WHERE 1=1")
            conn.commit()
            ConnectionPool.close(conn, curr)
            return True

    except Exception as e:
        ConnectionPool.close(conn, curr)
        log("Couldn't get agent count.", "ERROR", remote=True)
        log(repr(e), "ERROR", indentLevel=1, remote=True)
        return False


def connect():
    try:
        log("Connecting to database [" +
            config["postgres"]["host"] + "]", "INFO")

        ConnectionPool()

        return True

    except psycopg2.Error as e:
        log("Could not connect to postgres.", "ERROR")
        print(str(e))
        print(str(e.pgerror))
        return False


def clean():
    try:
        conn, curr = ConnectionPool.connect()
        curr.execute(
            "DELETE FROM wxdata.log WHERE timestamp < now() - interval '" + str(config["retentionDays"]) + " days'")
        conn.commit()
        curr.execute(
            "DELETE FROM wxdata.run_status WHERE timestamp < now() - interval '" + str(config["retentionDays"]) + " days'")
        conn.commit()
        ConnectionPool.close(conn, curr)
    except psycopg2.Error as e:
        ConnectionPool.close(conn, curr)
        log(f"· Couldn't delete old logs.",
            "WARN", indentLevel=0, remote=True)
        log(str(e), "WARN", indentLevel=0, remote=True)
        log(str(e.pgerror), "WARN", indentLevel=0, remote=True)
