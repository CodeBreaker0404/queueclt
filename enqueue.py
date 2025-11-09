# enqueue.py
import sys
import json
from queue.helper import make_job
from queue.db import insert_job

def main():
    if len(sys.argv) < 2:
        print("Usage: python enqueue.py <command> [payload_json]", file=sys.stderr)
        print("Example CLI job: python enqueue.py \"python jobs/add.py 2 3\"")
        print("Example Python job: python enqueue.py jobs.add.run '{\"a\":2,\"b\":3}'")
        sys.exit(1)

    command = sys.argv[1]
    payload = None

    if len(sys.argv) >= 3:
        try:
            payload = json.loads(sys.argv[2])
        except json.JSONDecodeError:
            print("Invalid JSON payload, using None.", file=sys.stderr)
            payload = None

    job = make_job(command, payload=payload)
    insert_job(job)
    print(f"Enqueued job: {job.id}")
    print(f"Command: {job.command}")
    if payload:
        print(f"Payload: {payload}")

if __name__ == "__main__":
    main()
