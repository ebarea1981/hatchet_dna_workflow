from hatchet_sdk import Hatchet, Context, DurableContext
from pydantic import BaseModel
from typing import Dict, List
from datetime import timedelta
import asyncio
import os

# Initialize Hatchet client
hatchet = Hatchet(debug=True)

# Shared input model for dna_preprocessing tasks
class DNAAlignmentInput(BaseModel):
    sample_id: str  # Unique identifier for the sample
    input_fastq: str  # Path to input FASTQ file
    reference_genome: str  # Path to reference genome
    output_dir: str  # Output directory for task outputs

# Input model for parent workflow
class BatchPreprocessingInput(BaseModel):
    control_samples: List[DNAAlignmentInput]  # List of control sample inputs
    other_samples: List[DNAAlignmentInput]   # List of non-control (e.g., treatment) sample inputs

# Output models for dna_preprocessing tasks
class AlignmentOutput(BaseModel):
    bam_file: str  # Path to the generated BAM file

class MethylDackelOutput(BaseModel):
    methylation_data: str  # Path to the methylation data file

# Output model for reference task
class ReferenceOutput(BaseModel):
    reference_file: str  # Path to the reference methylation file

# Output model for parent workflow
class BatchPreprocessingOutput(BaseModel):
    results: Dict[str, MethylDackelOutput]  # Map of sample_id to methylation data
    reference: ReferenceOutput  # Reference sample data

# Child workflow: dna_preprocessing
dna_preprocessing = hatchet.workflow(name="DNAPreprocessing", input_validator=DNAAlignmentInput)

@dna_preprocessing.durable_task(execution_timeout="4h")
async def dna_alignment_task(input: DNAAlignmentInput, ctx: DurableContext) -> Dict[str, str]:
    """
    Durable task to simulate NVIDIA Clara Parabricks for DNA alignment.
    """
    try:
        ctx.log(f"Starting DNA alignment for sample {input.sample_id}")
        
        # Ensure idempotency: Check if output already exists
        output_bam = os.path.join(input.output_dir, f"{input.sample_id}_aligned.bam")
        if os.path.exists(output_bam):
            ctx.log(f"Output {output_bam} already exists, skipping alignment")
            return AlignmentOutput(bam_file=output_bam).dict()
        
        # Simulate Parabricks command
        cmd = [
            "pbrun", "fq2bam",
            "--ref", input.reference_genome,
            "--in-fq", input.input_fastq,
            "--out-bam", output_bam,
            "--num-gpus", "1"
        ]
        
        ctx.log(f"Would run command: {' '.join(cmd)}")
        ctx.log("Simulating alignment process...")
        await ctx.aio_sleep_for(duration=timedelta(seconds=5))
        
        ctx.log(f"Alignment completed, output: {output_bam}")
        return AlignmentOutput(bam_file=output_bam).dict()
    
    except Exception as e:
        ctx.log(f"Error in DNA alignment for sample {input.sample_id}: {str(e)}")
        raise

@dna_preprocessing.durable_task(parents=[dna_alignment_task], execution_timeout="1h")
async def methyldackel_task(input: DNAAlignmentInput, ctx: DurableContext) -> Dict[str, str]:
    """
    Durable task to simulate MethylDackel extract command.
    """
    try:
        # Get parent task output
        alignment_output = await ctx.task_output(dna_alignment_task)
        bam_file = alignment_output["bam_file"]
        ctx.log(f"Using BAM file for sample {input.sample_id}: {bam_file}")
        
        # Ensure idempotency
        output_methyl = os.path.join(input.output_dir, f"{input.sample_id}_methylation.bed")
        if os.path.exists(output_methyl):
            ctx.log(f"Output {output_methyl} already exists, skipping MethylDackel")
            return MethylDackelOutput(methylation_data=output_methyl).dict()
        
        # Simulate MethylDackel command
        cmd = [
            "MethylDackel", "extract",
            bam_file,
            "-o", output_methyl
        ]
        
        ctx.log(f"Would run command: {' '.join(cmd)}")
        ctx.log("Simulating MethylDackel process...")
        await ctx.aio_sleep_for(duration=timedelta(seconds=3))
        
        ctx.log(f"MethylDackel completed, output: {output_methyl}")
        return MethylDackelOutput(methylation_data=output_methyl).dict()
    
    except Exception as e:
        ctx.log(f"Error in MethylDackel for sample {input.sample_id}: {str(e)}")
        raise

# Parent workflow: DNABatchPreprocessing
batch_preprocessing = hatchet.workflow(name="DNABatchPreprocessing", input_validator=BatchPreprocessingInput)

@batch_preprocessing.durable_task(execution_timeout="5h")
async def spawn_preprocessing_tasks(input: BatchPreprocessingInput, ctx: DurableContext) -> Dict[str, Dict[str, str]]:
    """
    Durable task to run dna_preprocessing workflows for each sample in parallel.
    """
    try:
        ctx.log("Starting batch preprocessing for all samples")
        
        # Combine control and other samples for processing
        all_samples = input.control_samples + input.other_samples
        
        # Prepare bulk run items for parallel execution
        bulk_run_items = [
            dna_preprocessing.create_bulk_run_item(
                input=sample,  # Pass model instance directly
                key=f"preprocessing-{sample.sample_id}"
            )
            for sample in all_samples
        ]
        
        ctx.log(f"Running {len(bulk_run_items)} child workflows in parallel")
        # Run child workflows in parallel using aio_run_many
        results = await dna_preprocessing.aio_run_many(bulk_run_items)
        
        # Process results
        aggregated_results = {}
        for result, sample in zip(results, all_samples):
            try:
                ctx.log(f"Child workflow for sample {sample.sample_id} completed")
                aggregated_results[sample.sample_id] = result["dna_alignment_task"] | result["methyldackel_task"]
            except Exception as e:
                ctx.log(f"Child workflow for sample {sample.sample_id} failed: {str(e)}")
                raise
        
        ctx.log("All child workflows completed")
        return aggregated_results
    
    except Exception as e:
        ctx.log(f"Error in running child workflows: {str(e)}")
        raise

@batch_preprocessing.durable_task(parents=[spawn_preprocessing_tasks], execution_timeout="1h")
async def create_reference_task(input: BatchPreprocessingInput, ctx: DurableContext) -> Dict[str, str]:
    """
    Durable task to create a reference sample (centroid) from control samples.
    """
    try:
        ctx.log("Starting creation of reference sample from control samples")
        
        # Get preprocessing results
        preprocessing_results = await ctx.task_output(spawn_preprocessing_tasks)
        
        # Ensure idempotency: Check if reference file exists
        reference_file = "/path/to/output/reference_methylation.bed"
        if os.path.exists(reference_file):
            ctx.log(f"Reference file {reference_file} already exists, skipping creation")
            return ReferenceOutput(reference_file=reference_file).dict()
        
        # Collect methylation data from control samples
        control_methylation_files = [
            preprocessing_results[sample.sample_id]["methylation_data"]
            for sample in input.control_samples
        ]
        
        ctx.log(f"Control methylation files: {control_methylation_files}")
        
        # Simulate reference creation (e.g., averaging methylation profiles)
        cmd = [
            "methylation_centroid_tool",  # Placeholder command
            "--inputs", ",".join(control_methylation_files),
            "--output", reference_file
        ]
        
        ctx.log(f"Would run command: {' '.join(cmd)}")
        ctx.log("Simulating reference creation process...")
        await ctx.aio_sleep_for(duration=timedelta(seconds=5))
        
        ctx.log(f"Reference creation completed, output: {reference_file}")
        return ReferenceOutput(reference_file=reference_file).dict()
    
    except Exception as e:
        ctx.log(f"Error in reference creation: {str(e)}")
        raise

@batch_preprocessing.durable_task(parents=[create_reference_task], execution_timeout="1h")
async def post_processing_task(input: BatchPreprocessingInput, ctx: DurableContext) -> Dict[str, Dict[str, str]]:
    """
    Durable task to process results from all samples using the reference sample.
    """
    try:
        # Get results from spawn_preprocessing_tasks and create_reference_task
        preprocessing_results = await ctx.task_output(spawn_preprocessing_tasks)
        reference_result = await ctx.task_output(create_reference_task)
        reference_file = reference_result["reference_file"]
        ctx.log(f"Starting post-processing with reference: {reference_file}")
        
        # Combine all samples
        all_samples = input.control_samples + input.other_samples
        
        # Aggregate methylation data with reference (placeholder logic)
        aggregated_results = {}
        for sample in all_samples:
            sample_id = sample.sample_id
            methylation_data = preprocessing_results[sample_id]["methylation_data"]
            ctx.log(f"Processing methylation data for sample {sample_id}: {methylation_data} against reference")
            # Simulate computation with reference
            aggregated_results[sample_id] = MethylDackelOutput(methylation_data=methylation_data).dict()
        
        ctx.log("Post-processing completed")
        return {
            "results": aggregated_results,
            "reference": ReferenceOutput(reference_file=reference_file).dict()
        }
    
    except Exception as e:
        ctx.log(f"Error in post-processing: {str(e)}")
        raise

def start_worker():
    worker = hatchet.worker("test-worker", slots=1, workflows=[dna_preprocessing, batch_preprocessing])
    worker.start()

# Example usage
async def run_batch_workflow():
    # Define 6 samples (3 control, 3 treatment)
    control_samples = [
        DNAAlignmentInput(
            sample_id="control_1",
            input_fastq="/path/to/control_1.fastq",
            reference_genome="/path/to/ref.fasta",
            output_dir="/path/to/output/control_1"
        ),
        DNAAlignmentInput(
            sample_id="control_2",
            input_fastq="/path/to/control_2.fastq",
            reference_genome="/path/to/ref.fasta",
            output_dir="/path/to/output/control_2"
        ),
        DNAAlignmentInput(
            sample_id="control_3",
            input_fastq="/path/to/control_3.fastq",
            reference_genome="/path/to/ref.fasta",
            output_dir="/path/to/output/control_3"
        )
    ]
    other_samples = [
        DNAAlignmentInput(
            sample_id="treatment_1",
            input_fastq="/path/to/treatment_1.fastq",
            reference_genome="/path/to/ref.fasta",
            output_dir="/path/to/output/treatment_1"
        ),
        DNAAlignmentInput(
            sample_id="treatment_2",
            input_fastq="/path/to/treatment_2.fastq",
            reference_genome="/path/to/ref.fasta",
            output_dir="/path/to/output/treatment_2"
        ),
        DNAAlignmentInput(
            sample_id="treatment_3",
            input_fastq="/path/to/treatment_3.fastq",
            reference_genome="/path/to/ref.fasta",
            output_dir="/path/to/output/treatment_3"
        )
    ]
    
    # result = await hatchet.run_workflow("DNABatchPreprocessing", input_data)
    input_data = BatchPreprocessingInput(control_samples=control_samples, other_samples=other_samples)
    result = await batch_preprocessing.aio_run(input_data)
    print(f"Batch workflow result: {result}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(run_batch_workflow())