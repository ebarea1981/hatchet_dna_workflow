# from hatchet_dna_workflow.workflows.hatchet_step_processing_with_result import start_worker
from hatchet_dna_workflow.workflows.preprocess_workflow_updated import hatchet, flow_preprocess_sample, subflow_readcounts

def start_worker():
    worker = hatchet.worker("preprocess-worker", workflows=[flow_preprocess_sample, subflow_readcounts])
    worker.start()

if __name__ == "__main__":
    start_worker()