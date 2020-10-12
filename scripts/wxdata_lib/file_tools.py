from .config import config
from .logger import log
from . import pg_connection_manager as pg

import os


def clean():
    retention_days = str(config["retentionDays"])
    log(f"· Deleting rasters from {config['mapfileDir']} older than {retention_days} days.",
        "DEBUG", indentLevel=0)
    try:
        os.system(
            f'find {config["mapfileDir"]}/*/* -mtime +' + retention_days + ' -exec rm -f {} \;')
        os.system(f'find {config["tempDir"]}/* -mtime +' +
                  retention_days + ' -exec rm -f {} \;')
    except:
        log(f"· Couldn't delete old rasters from {config['mapfileDir']}.",
            "WARN", indentLevel=0, remote=True)
