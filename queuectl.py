# queuectl.py
import argparse
import json
import sys
from db import Database
import worker as worker_mod
import os


def cmd_enqueue(args):
    db = Database()
    if args.command:
        cmd = args.command
    elif args.json:
        data = json.loads(args.json)
        cmd = data.get("command")
        if not cmd:
            print("JSON must contain 'command' field.")
            return
    else:
        print("Provide --command or --json")
        return

    job_id = db.add_job(command=cmd, job_id=args.id, max_retries=args.max_retries)
    print("Enqueued:", job_id)

def cmd_list(args):
    db = Database()
    rows = db.list_jobs(state=args.state, limit=args.limit)
    if not rows:
        print("No jobs found.")
        return
    for r in rows:
        print(f"{r['id']} | {r['state']} | attempts={r['attempts']} | max_retries={r['max_retries']} | cmd={r['command']} | available_at={r['available_at']}")

def cmd_status(args):
    db = Database()
    counts = db.get_counts()
    print("Job counts:")
    for k in ["pending","processing","completed","dead"]:
        print(f"  {k:10s}: {counts.get(k,0)}")

def cmd_config(args):
    db = Database()
    if getattr(args, "set_key", None) and getattr(args, "set_value", None) is not None:
        db.set_config(args.set_key, args.set_value)
        print("Set", args.set_key, "=", args.set_value)
    else:
        for k in ("max_retries","backoff_base"):
            val = db.get_config(k)
            print(f"{k} = {val}")

def cmd_worker(args):
    if args.action == "start":
        count = args.count or 1
        print(f"Starting {count} worker(s)... (this process will block — run in a separate terminal if you want to continue issuing commands)")
        worker_mod.start_workers(count=count)
    elif args.action == "stop":
        print("Stopping workers...")
        worker_mod.stop_workers()
    else:
        print("Unknown worker action; use start or stop.")

def cmd_dlq(args):
    db = Database()
    if args.action == "list":
        rows = db.list_jobs(state="dead", limit=args.limit)
        if not rows:
            print("DLQ empty.")
            return
        for r in rows:
            print(f"{r['id']} | attempts={r['attempts']} | cmd={r['command']}")
    elif args.action == "retry":
        if not args.job_id:
            print("Provide --job-id to retry.")
            return
        j = db.get_job(args.job_id)
        if not j:
            print("Job not found:", args.job_id)
            return
        db.reset_job_to_pending(args.job_id)
        print("Job reset to pending:", args.job_id)
    else:
        print("Unknown dlq action.")

def cmd_logs(args):
    log_file = f"logs/{args.job_id}.log"
    if not os.path.exists(log_file):
        print("No log found for job:", args.job_id)
        return
    with open(log_file, "r", encoding="utf-8") as f:
        print(f.read())

def cmd_stats(args):
    db = Database()
    counts = db.get_counts()

    total = sum(counts.values())

    rows = db.list_jobs(limit=100000)
    if rows:
        avg_attempts = sum(int(r['attempts']) for r in rows) / len(rows)
    else:
        avg_attempts = 0

    workers_running = 0
    if os.path.exists("workers.json"):
        import json
        with open("workers.json") as f:
            data = json.load(f)
            workers_running = len(data.get("pids", []))

    print("== Metrics ==")
    print("Total jobs:", total)
    print("Completed:", counts.get("completed", 0))
    print("Dead:", counts.get("dead", 0))
    print("Pending:", counts.get("pending", 0))
    print("Processing:", counts.get("processing", 0))
    print("Average attempts:", round(avg_attempts, 2))
    print("Workers running:", workers_running)


def main():
    parser = argparse.ArgumentParser(prog="queuectl")
    sub = parser.add_subparsers(dest="cmd")

    p_enq = sub.add_parser("enqueue", help="Enqueue a job")
    p_enq.add_argument("--command", "-c", help="command to run (shell)")
    p_enq.add_argument("--json", help='json string like \'{"command":"echo hi"}\'')
    p_enq.add_argument("--id", help="optional job id")
    p_enq.add_argument("--max-retries", type=int, help="override max retries for this job")

    p_list = sub.add_parser("list", help="List jobs")
    p_list.add_argument("--state", choices=["pending","processing","completed","dead"], help="filter by state")
    p_list.add_argument("--limit", type=int, default=100)

    p_status = sub.add_parser("status", help="Show job counts")

    p_cfg = sub.add_parser("config", help="Get or set config")
    p_cfg.add_argument("--set", nargs=2, metavar=("KEY","VALUE"), dest=("set_pair"), help="set key value (e.g. --set max_retries 3)")
    p_cfg.add_argument("--set-key", help=argparse.SUPPRESS)
    p_cfg.add_argument("--set-value", help=argparse.SUPPRESS)

    p_worker = sub.add_parser("worker", help="Manage worker processes")
    p_worker.add_argument("action", choices=["start","stop"], help="start or stop workers")
    p_worker.add_argument("--count", type=int, default=1, help="number of worker processes to spawn (start)")

    p_dlq = sub.add_parser("dlq", help="Dead Letter Queue commands")
    p_dlq.add_argument("action", choices=["list","retry"], help="list DLQ or retry a job")
    p_dlq.add_argument("--job-id", help="job id to retry")
    p_dlq.add_argument("--limit", type=int, default=100)

    p_logs = sub.add_parser("logs", help="Show job logs")
    p_logs.add_argument("--job-id", required=True)

    p_stats = sub.add_parser("stats", help="Show execution metrics")



    args = parser.parse_args()
    if args.cmd == "config" and getattr(args, "set_pair", None):
        args.set_key, args.set_value = args.set_pair

    if args.cmd == "enqueue":
        cmd_enqueue(args)
    elif args.cmd == "list":
        cmd_list(args)
    elif args.cmd == "status":
        cmd_status(args)
    elif args.cmd == "config":
        cmd_config(args)
    elif args.cmd == "worker":
        cmd_worker(args)
    elif args.cmd == "dlq":
        cmd_dlq(args)
    elif args.cmd == "logs":
        cmd_logs(args)
    elif args.cmd == "stats":
        cmd_stats(args)
    else:
        parser.print_help()
    

if __name__ == "__main__":
    main()
