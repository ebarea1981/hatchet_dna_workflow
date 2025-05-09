from hatchet_sample.workflows import dna_workflow
from hatchet_sample import hatchet

# Define workers with specific labels for GPU
def start_gpu_worker(dna_workflow, slots=1):
    gpu_worker = hatchet.worker(
        name='gpu-worker',
        slots=slots,  # Number of concurrent tasks the worker can handle
        labels={
            'gpu': 'nvidia',
            'memory': 512  # Moderate memory for GPU worker
        },
        workflows=[dna_workflow]  # Associate the workflow instance with this worker
    )
    print(f"Starting GPU worker with {slots} slots...")
    gpu_worker.start()

if __name__ == "__main__":
    start_gpu_worker(dna_workflow, slots=1)