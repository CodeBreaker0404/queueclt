# main.py

import argparse
import json
from queue.manager import QueueManager
from queue.worker import Worker
from queue.dlq import DLQ
from queue.config import load_config
from queue.utils import logger

def main():
    load_config()  # ensure config loaded
    qm = QueueManager()
    dlq = DLQ()

    parser = argparse.ArgumentParser(
        prog="queuectl",
        description="lightweight background job queue"
    )

    sub = parser.add_subparsers(dest="command")

    # --------------------------
    # enqueue
    # --------------------------
    p_enqueue = sub.add_parser("enqueue", help="Add a new job")
    p_enqueue.add_argument("--python", action="store_true",
                           help="Execute job using Python handler")
    p_enqueue.add_argument("job_name", help="Job name (module.func)")
    p_enqueue.add_argument("--payload", type=str,
                           help="JSON string of arguments for Python job")
    p_enqueue.add_argument("args", nargs="*", help="Arguments for CLI job")

    # --------------------------
    # list
    # --------------------------
    sub.add_parser("list", help="List all jobs")

    # --------------------------
    # start workers
    # --------------------------
    p_sw = sub.add_parser("start-workers", help="Start worker")
    p_sw.add_argument("--poll", type=int, default=None)

    # --------------------------
    # dlq
    # --------------------------
    p_dlq = sub.add_parser("dlq", help="DLQ operations")
    p_dlq.add_argument("action", choices=["list", "retry", "purge"])
    p_dlq.add_argument("--id", type=int)

    args = parser.parse_args()

    # ----------------------------------------------------------
    # Commands
    # ----------------------------------------------------------

    if args.command == "enqueue":
        if args.python:
            # For Python jobs, pass payload JSON string
            payload_dict = json.loads(args.payload) if args.payload else {}
            cmd = args.job_name
            job_id = qm.enqueue(cmd, payload=payload_dict, use_python=True)
        else:
            # For CLI jobs
            cmd = " ".join([args.job_name] + args.args)
            job_id = qm.enqueue(cmd, use_python=False)

        logger.info(f"Enqueued job id={job_id} (python={args.python}) -> {cmd}")

    elif args.command == "list":
        for job in qm.list_jobs():
            print(job.to_dict())

    elif args.command == "start-workers":
        worker = Worker(poll_interval=args.poll)
        worker.start()

    elif args.command == "dlq":
        if args.action == "list":
            for j in dlq.list():
                print(j)
        elif args.action == "retry":
            if not args.id:
                print("Need --id")
                return
            ok = dlq.retry(args.id)
            print("OK" if ok else "Failed")
        elif args.action == "purge":
            n = dlq.purge()
            print(f"Purged {n}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
