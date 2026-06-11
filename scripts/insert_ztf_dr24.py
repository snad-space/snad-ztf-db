# /// script
# requires-python = ">=3.14"
# dependencies = [
#   "s3fs",
#   "universal_pathlib",
# ]
# ///

import re
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from upath import UPath

# --- Configuration ---
DR = 24
BASE_URL = UPath(
    f"s3://ipac-irsa-ztf/ztf/enhanced/dr{DR}/lc/hats/ztf_dr{DR}_lc-hats/",
    anon=True,
)
MD5_URL = BASE_URL / "md5sums.txt"
THREADS = 4
INSERTED_LOG = "inserted.csv"
ERROR_LOG = "errors.txt"

# Threading lock to prevent interleaved writes to the log files
file_lock = threading.Lock()

def s3_to_https(path: UPath) -> str:
    return f"https://{path.drive}.s3.amazonaws.com/{path.relative_to(path.anchor)}"

def process_file(line):
    """
    Parses a single line from the checksum file, extracts metadata, 
    and runs the ClickHouse insert via Docker.
    """
    line = line.strip()
    if not line:
        return

    # 1. Parse the checksum line (Format: <md5> <relative_path>)
    try:
        _md5, rel_path = line.split()
    except ValueError:
        return f"Error: Malformed line: {line}"

    if not rel_path.endswith(".parquet"):
        return

    # 2. Extract metadata from filename using Regex
    url = BASE_URL / rel_path
    
    print(f"Inserting {url}")

    # 3. Construct the ClickHouse Query
    # Note: We use the variables extracted from the filename
    query = f"""
INSERT INTO ztf.dr{DR}_olc
SETTINGS max_memory_usage = '180G', max_insert_threads = 10, connect_timeout = 30000
SELECT
    oid, filter, fieldid, rcid, ra, dec, nobs_w_bad,
    arrayFilter((_, is_good) -> is_good, mjd, mask) AS mjd,
    arrayFilter((_, is_good) -> is_good, mag, mask) AS mag,
    arrayFilter((_, is_good) -> is_good, magerr, mask) AS magerr,
    arrayFilter((_, is_good) -> is_good, clrcoeff, mask) AS clrcoeff
FROM (
    SELECT
        toUInt64(objectid) AS oid,
        toUInt8(filterid) AS filter,
        toUInt16(intDiv(oid, 1_000_000_000_000)) AS fieldid,
        toUInt8(intDiv(oid, 1_000_000_000) % 100) as ccdid,
        toUInt8(intDiv(oid, 100_000_000) % 10) as qid,
        (ccdid-1)*4 + qid-1 AS rcid,
        toFloat64(objra) AS ra,
        toFloat64(objdec) AS dec,
        length(lightcurve.hmjd) AS nobs_w_bad,
        lightcurve.hmjd AS mjd,
        lightcurve.mag AS mag,
        lightcurve.magerr AS magerr,
        lightcurve.clrcoeff AS clrcoeff,
        lightcurve.catflags AS catflags,
        arrayMap((time, magnitude, err, flag) -> (isNotNull(time) AND isNotNull(magnitude) AND (err > 0) AND (flag = 0)), mjd, mag, magerr, catflags) AS mask
    FROM s3('{s3_to_https(url)}', NOSIGN, 'Parquet')
    )
WHERE length(mag) > 0
SETTINGS max_memory_usage = '40G', max_insert_threads = 10, connect_timeout = 30000
    """

    # 4. Execute via Docker
    # We use -i (interactive) to pipe the query safely to the client
    docker_cmd = [
        "docker", "exec", "-i", "clickhouse", 
        "clickhouse-client",
        "--max_execution_time", "36000",
        "--receive_timeout", "36000",
        "--connect_timeout", "36000",
        "--query", query,
    ]

    try:
        subprocess.run(docker_cmd, capture_output=True, text=True, check=True)
        
        # 5. Thread-Safe Logging for Success
        with file_lock:
            with open(INSERTED_LOG, "a") as f:
                f.write(f"{rel_path}\n")
        return f"Success: {rel_path}"

    except subprocess.CalledProcessError as e:
        # 6. Thread-Safe Logging for Failure
        with file_lock:
            with open(ERROR_LOG, "a") as f:
                f.write(f"FAILED: {rel_path}\nError: {e.stderr}\n---\n")
        return f"Failed: {rel_path}"

def main():
    print(f"Fetching checksum file from {BASE_URL}...")
    
    try:
        content = (BASE_URL / "md5sums.txt").read_text()
        lines = [l for l in content.splitlines() if l.strip()]
    except Exception as e:
        print(f"Error fetching checksum.md5: {e}")
        return

    print(f"Found {len(lines)} files. Starting ingestion with {THREADS} threads...")

    # Run the pool
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        # Using list() to force execution of the generator
        for result in executor.map(process_file, lines):
            print(result)

    print(f"\nProcessing finished.")
    print(f"Successfully inserted records logged to: {INSERTED_LOG}")
    print(f"Failed attempts logged to: {ERROR_LOG}")

if __name__ == "__main__":
    main()
