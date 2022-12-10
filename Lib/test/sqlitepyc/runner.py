import sys
import time
import subprocess

FS_TIMES = []
DB_TIMES = []

def run(use_sqlite_pycache):
    args = [sys.executable]
    if use_sqlite_pycache:
        args += ["-f"]
    args += ["-m", "test.sqlitepyc.importer"]
    start_time = time.time()
    code = subprocess.run(args).returncode
    end_time = time.time()
    assert code == 99
    duration = int((end_time - start_time) * 1000)
    if use_sqlite_pycache:
        kind = "DB"
        DB_TIMES.append(duration)
    else:
        kind = "FS"
        FS_TIMES.append(duration)
    print(f"{kind} {duration} ms")
    time.sleep(0.2)

def main():
    run(True)
    run(False)

    FS_TIMES.clear()
    DB_TIMES.clear()

    for i in range(200):
        run(False)
    print()

    for i in range(200):
        run(True)
    print()

    for i in range(200):
        run(False)
        run(True)
    print()

    print("FS_TIMES =", FS_TIMES)
    print("DB_TIMES =", DB_TIMES)

if __name__ == "__main__":
    main()
