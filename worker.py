# worker.py
import time
import subprocess
import signal
import os
import json
import sys
from multiprocessing import Process
from db import Database

WORKERS_FILE = "workers.json"
shutdown = False

def _signal_handler(signum, frame):
    global shutdown
    shutdown = True

def worker_loop(worker_name: str):
    global shutdown
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    db = Database()
    try:
        backoff_base = int(db.get_config("backoff_base") or 2)
    except Exception:
        backoff_base = 2

    while not shutdown:
        job = db.claim_job()
        if not job:
            time.sleep(1)
            continue

        job_id = job["id"]
        cmd = job["command"]
        attempts = int(job["attempts"])
        max_retries = int(job["max_retries"])

        print(f"[{worker_name}] Running job {job_id} (attempt {attempts}/{max_retries}) -> {cmd}")
        try:
            proc = subprocess.run(["/bin/bash", "-lc", cmd])
            rc = proc.returncode
        except FileNotFoundError:
            proc = subprocess.run(cmd,shell=True,capture_output=True,text=True)
            rc = proc.returncode
            os.makedirs("logs", exist_ok=True)
            log_path = f"logs/{job_id}.log"
            with open(log_path, "w", encoding="utf-8") as f:
                f.write("COMMAND:\n")
                f.write(cmd + "\n\n")
                f.write("STDOUT:\n")
                f.write(proc.stdout or "")
                f.write("\n\nSTDERR:\n")
                f.write(proc.stderr or "")

        except Exception as e:
            print(f"[{worker_name}] Error running job {job_id}: {e}")
            rc = 1

        if rc == 0:
            db.set_job_completed(job_id)
            print(f"[{worker_name}] Job {job_id} completed.")
        else:
            if attempts >= max_retries:
                db.set_job_dead(job_id)
                print(f"[{worker_name}] Job {job_id} moved to DLQ after {attempts} attempts.")
            else:
                db.reschedule_job_with_backoff(job_id, attempts=attempts, backoff_base=backoff_base)
                print(f"[{worker_name}] Job {job_id} failed; rescheduled with backoff (attempt {attempts}).")

    print(f"[{worker_name}] shutting down")

def start_workers(count: int = 1):
    procs = []
    proc_objs = []
    for i in range(count):
        name = f"worker-{i+1}"
        p = Process(target=worker_loop, args=(name,), daemon=False)
        p.start()
        proc_objs.append(p)
        procs.append(p.pid)
        print(f"Started {name} pid={p.pid}")

    try:
        with open(WORKERS_FILE, "w") as f:
            json.dump({"pids": procs}, f)
    except Exception:
        pass

    try:
        for p in proc_objs:
            while p.is_alive():
                p.join(timeout=0.5)
    except KeyboardInterrupt:
        print("Received interrupt, stopping workers...")
        stop_workers()
    except Exception as e:
        print("Parent loop error:", e)
        stop_workers()

def stop_workers():
    if not os.path.exists(WORKERS_FILE):
        print("No workers.json found; nothing to stop.")
        return
    try:
        with open(WORKERS_FILE, "r") as f:
            data = json.load(f)
            pids = data.get("pids", [])
    except Exception as e:
        print("Could not read workers file:", e)
        pids = []

    for pid in pids:
        try:
            try:
                os.kill(pid, signal.SIGTERM)
            except Exception:
                if sys.platform.startswith("win"):
                    os.system(f"taskkill /PID {pid} /F >nul 2>&1")
                else:
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except Exception:
                        pass
            print("Stopped pid", pid)
        except Exception as e:
            print("Failed to stop pid", pid, e)

    try:
        os.remove(WORKERS_FILE)
    except Exception:
        pass
