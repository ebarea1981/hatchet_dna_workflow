from hatchet_sample.workflows import dna_workflow
from hatchet_sample import hatchet

# Define workers with specific labels for high memory
def start_high_memory_worker(dna_workflow, slots=5):
    high_memory_worker = hatchet.worker(
        name='high-memory-worker',
        slots=slots,  # Number of concurrent tasks the worker can handle
        labels={
            'gpu': 'none',  # No GPU
            'memory': 2048  # High memory for this worker
        },
        workflows=[dna_workflow]  # Associate the workflow instance with this worker
    )
    print(f"Starting high memory worker with {slots} slots...")
    high_memory_worker.start()

if __name__ == "__main__":
    start_high_memory_worker(dna_workflow, slots=5)