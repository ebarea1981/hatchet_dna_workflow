# DNA Batch Preprocessing Workflow

## Overview
This project implements a Hatchet-based workflow for preprocessing multiple DNA samples (control and treatment groups) in parallel, followed by reference sample creation and post-processing. The workflow is designed for DNA alignment and methylation extraction, with a focus on scalability and reliability using Hatchet's durable task execution.

The workflow processes FASTQ files through NVIDIA Clara Parabricks for alignment and MethylDackel for methylation extraction, creates a reference sample from control samples, and performs post-processing (e.g., differential methylation analysis). The current implementation uses simulation mode (logging commands and sleeping) for prototyping, with placeholders for production commands.

## Workflow Structure
The project consists of two Hatchet workflows:

### 1. `dna_preprocessing`
- **Purpose**: Processes a single DNA sample through alignment and methylation extraction.
- **Input**: `DNAAlignmentInput` (Pydantic model with `sample_id`, `input_fastq`, `reference_genome`, `output_dir`).
- **Tasks**:
  - `dna_alignment_task`: Simulates NVIDIA Clara Parabricks (`pbrun fq2bam`) to generate a BAM file (`execution_timeout="4h"`, `retries=3`).
  - `methyldackel_task`: Simulates MethylDackel (`extract`) to produce a methylation BED file (`execution_timeout="1h"`, `retries=3`).
- **Output**: `MethylDackelOutput` with the methylation data file path.

### 2. `batch_preprocessing`
- **Purpose**: Orchestrates parallel preprocessing of multiple samples, creates a reference sample from control samples, and performs post-processing.
- **Input**: `BatchPreprocessingInput` (Pydantic model with `control_samples` and `other_samples`, each a list of `DNAAlignmentInput`).
- **Tasks**:
  - `spawn_preprocessing_tasks`: Runs `dna_preprocessing` workflows in parallel for all samples using `aio_run_many` (`execution_timeout="5h"`, `retries=3`).
  - `create_reference_task`: Aggregates control samples' methylation data into a reference file (simulated, `execution_timeout="1h"`, `retries=3`).
  - `post_processing_task`: Processes all samples' methylation data against the reference (placeholder for differential analysis, `execution_timeout="1h"`, `retries=3`).
- **Output**: `BatchPreprocessingOutput` with a dictionary of sample results and the reference file.

## Setup Instructions
### Prerequisites
- Python 3.8+
- Hatchet SDK (`hatchet-sdk`)
- Pydantic 2.x
- Access to a shared filesystem or cloud storage for output files
- (For production) NVIDIA Clara Parabricks Docker image and GPU-enabled workers

### Installation
1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Set Up a Virtual Environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**:
   ```bash
   pip install hatchet-sdk pydantic
   ```

4. **Verify Versions**:
   ```bash
   pip show hatchet-sdk pydantic
   ```
   Ensure `pydantic>=2.0` and the latest `hatchet-sdk`.

## Usage
The workflow runs in prototyping mode, simulating commands with logging and sleep delays. Follow these steps to execute it:

### 1. Start the Hatchet Worker
- In a terminal, run the worker to process workflows:
  ```bash
  python dna_batch_preprocessing_workflow.py
  ```
- Alternatively, create a separate `worker.py`:
  ```python
  import logging
  from dna_batch_preprocessing_workflow import start_worker

  logging.basicConfig(level=logging.INFO)
  logger = logging.getLogger(__name__)

  if __name__ == "__main__":
      logger.info("Starting worker script")
      start_worker()
  ```
  Run:
  ```bash
  python worker.py
  ```
- The worker uses 6 slots for parallel execution of up to 6 `dna_preprocessing` workflows.

### 2. Run the Workflow
- In another terminal, execute the workflow:
  ```bash
  python dna_batch_preprocessing_workflow.py
  ```
- The workflow processes 6 samples (3 control, 3 treatment), creates a reference sample, and performs post-processing. Logs are output to the console and Hatchet dashboard.

### 3. Output
- Outputs are written to `/path/to/output/<sample_id>` (e.g., `control_1_aligned.bam`, `control_1_methylation.bed`).
- In prototyping mode, idempotency checks (`os.path.exists`) may return `False` unless dummy files are created. Remove these checks for testing:
  ```python
  # if os.path.exists(output_bam):
  #     ctx.log(f"Output {output_bam} already exists, skipping alignment")
  #     return AlignmentOutput(bam_file=output_bam).model_dump()
  ```
- The reference file is written to `/path/to/output/reference_methylation.bed`.

## Prototyping vs. Production
### Prototyping Mode
- **Current State**: The workflow simulates commands (e.g., `pbrun fq2bam`, `MethylDackel extract`, `methylation_centroid_tool`) by logging them and sleeping (5s for alignment, 3s for methylation, 5s for reference creation).
- **Purpose**: Allows testing the workflow structure without executing real commands.
- **Limitations**: No actual file processing; outputs are placeholder paths.

### Moving to Production
1. **Replace Simulation Logic**:
   - Update tasks to use `asyncio.create_subprocess_exec`:
     ```python
     process = await asyncio.create_subprocess_exec(
         *cmd,
         stdout=asyncio.subprocess.PIPE,
         stderr=asyncio.subprocess.PIPE
     )
     stdout, stderr = await process.communicate()
     if process.returncode != 0:
         ctx.log(f"Command failed: {stderr.decode()}")
         raise RuntimeError(f"Command failed: {stderr.decode()}")
     ```
   - Replace placeholder commands with actual ones (e.g., specify Parabricks and MethylDackel parameters).

2. **Configure GPU Support**:
   - Ensure the Hatchet worker runs in an environment with NVIDIA GPUs and the Parabricks Docker image (`nvcr.io/nvidia/clara-parabricks`).
   - Update worker configuration to allocate GPUs (https://docs.hatchet.run/home/workers).

3. **Implement Post-Processing**:
   - Add differential methylation analysis in `post_processing_task` using tools like `DSS` or `methylKit`:
     ```python
     cmd = [
         "Rscript", "differential_methylation.R",
         "--reference", reference_file,
         "--input", methylation_data,
         "--output", f"/path/to/output/{sample_id}_diff_methyl.bed"
     ]
     ```

4. **Storage**:
   - Ensure `/path/to/output` is a shared filesystem or cloud storage (e.g., S3) accessible to all tasks.

## Additional Notes
- **Monitoring**: Use Hatchet's dashboard to track workflow progress, task logs, and errors (https://docs.hatchet.run/home/logging).
- **Scalability**: The worker uses 6 slots for 6 parallel workflows. For larger datasets, adjust `slots` or add workers. Reintroduce task-level concurrency (`concurrency={"key": "gpu", "limit": 2}`) once the SDK's format is confirmed.
- **Error Handling**: Tasks include retries (`retries=3`) and idempotency checks to handle transient failures.
- **Future Improvements**:
  - Implement actual methylation analysis tools in `create_reference_task` and `post_processing_task`.
  - Add validation for output files in `post_processing_task`.
  - Create a separate `worker.py` script for cleaner worker execution.
  - Support dynamic concurrency based on GPU availability.

## Troubleshooting
- **Deprecation Warnings**: Ensure Pydantic 2.x is installed (`pip install --upgrade pydantic`). The workflow uses `model_dump()` for compatibility.
- **Missing Outputs**: If idempotency checks fail, remove `os.path.exists` checks or create dummy files for prototyping.
- **Worker Issues**: Verify the worker is running before executing the workflow. Check logs for registration errors.
- **Contact Support**: For SDK issues (e.g., concurrency parameter), report to Hatchet's GitHub (https://github.com/hatchet-dev/hatchet).

## Workflow Diagram
```
DNABatchPreprocessing Workflow
├── spawn_preprocessing_tasks
│   ├── dna_preprocessing (Child Workflow for each sample)
│   │   ├── dna_alignment_task
│   │   └── methyldackel_task (depends on dna_alignment_task)
│   └── (Runs in parallel for all samples)
├── create_reference_task (depends on spawn_preprocessing_tasks)
└── post_processing_task (depends on create_reference_task)
```