import urllib3
import certifi

http = urllib3.PoolManager(timeout=urllib3.Timeout(
    connect=5.0, read=10.0), cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())
