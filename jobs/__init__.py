import os
import importlib

def load_jobs():
    jobs = {}

    pkg = __name__  # "queue.jobs"
    folder = os.path.dirname(__file__)

    for file in os.listdir(folder):
        if not file.endswith(".py") or file.startswith("__"):
            continue

        module_name = file[:-3]  # drop .py
        module_path = f"{pkg}.{module_name}"

        module = importlib.import_module(module_path)

        # Each job file must define JOB_NAME and JOB_HANDLER
        if hasattr(module, "JOB_NAME") and hasattr(module, "JOB_HANDLER"):
            jobs[module.JOB_NAME] = module.JOB_HANDLER

    return jobs
