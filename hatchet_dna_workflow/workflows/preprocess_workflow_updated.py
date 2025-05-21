from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Type, Any, List, Optional, Dict
import os
from pathlib import Path
from datetime import datetime, timedelta
from hatchet_sdk import Hatchet, DurableContext, Context
from pydantic import BaseModel, model_validator, Field
import asyncio
import shutil
import gzip

# Initialize Hatchet client
hatchet = Hatchet()

# Type variables for generics
TInput = TypeVar("TInput", bound=BaseModel)
TOutput = TypeVar("TOutput", bound=BaseModel)


# Download/Upload Interfaces
class StorageClient(ABC):
    @abstractmethod
    def download_file(self, source_path: str, local_path: str) -> None:
        pass

    @abstractmethod
    def upload_file(self, local_path: str, destination_path: str) -> None:
        pass


# Concrete Storage Client (Local File System Example)
class LocalStorageClient(StorageClient):
    def download_file(self, source_path: str, local_path: str) -> None:
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"Source file {source_path} not found")
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(source_path, "rb") as src, open(local_path, "wb") as dst:
            dst.write(src.read())
        print(f"Downloaded {source_path} to {local_path}")

    def upload_file(self, local_path: str, destination_path: str) -> None:
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file {local_path} not found")
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        with open(local_path, "rb") as src, open(destination_path, "wb") as dst:
            dst.write(src.read())
        print(f"Uploaded {local_path} to {destination_path}")


# Pydantic Models
class BaseStepResult(BaseModel):
    status: str = Field(..., description="success|error|warning")
    metrics: Dict[str, Any] = Field(default_factory=dict)
    intermediate_files: Dict[str, str] = Field(default_factory=dict)
    error_details: Optional[str] = Field(default=None, description="Error details")
    output_file: Optional[str] = Field(default=None, description="Output file name")
    output_path: Optional[Path] = Field(default=None, description="Output file path")


class AlignResult(BaseStepResult):
    pass


class MethExtractionResult(BaseStepResult):
    pass


class ReadCountsResult(BaseStepResult):
    pass


class BaseStepParams(BaseModel):
    input_files: List[str] = Field(..., description="List of input file paths to download")


class AlignParams(BaseStepParams):
    sample_id: str = Field(..., description="Sample identifier")
    working_dir: Path = Field(..., description="Working directory for task execution")
    genomes_dir: Path = Field(..., description="Directory containing genome references")
    reference_file: str = Field(..., description="Reference genome file name")
    output_dir: Optional[Path] = Field(default=None, description="Directory for output files")
    num_cores: int = Field(default=10, description="Number of CPU cores to use")
    min_coverage: Optional[int] = Field(default=None, description="Minimum coverage threshold")
    logs_folder: Optional[Path] = Field(default=None, description="Directory for log files")

    def get_output_dir(self) -> Path:
        return self.output_dir if self.output_dir is not None else self.working_dir


class MethExtractionParams(BaseStepParams):
    sample_id: str = Field(..., description="Sample identifier")
    working_dir: Path = Field(..., description="Working directory for task execution")
    genomes_dir: Path = Field(..., description="Directory containing genome references")
    reference_file: str = Field(..., description="Reference genome file name")
    num_cores: int = Field(default=10, description="Number of CPU cores to use")
    output_prefix: Optional[str] = Field(default=None, description="Prefix for output files")
    output_dir: Optional[Path] = Field(default=None, description="Directory for output files")
    logs_folder: Optional[Path] = Field(default=None, description="Directory for log files")
    minMAPQ: int = Field(default=20, description="Minimum MAPQ threshold to include an alignment")
    minPhred: int = Field(default=20, description="Minimum Phred threshold to include a base")
    min_coverage: int = Field(default=4, description="Minimum coverage threshold to include a base")
    chromosome_list: Optional[List[str]] = Field(default=None, description="List of chromosomes to process")

    def get_output_dir(self) -> Path:
        return self.output_dir if self.output_dir is not None else self.working_dir


class ReadCountsParams(BaseStepParams):
    sample_id: str = Field(..., description="Sample identifier")
    working_dir: Path = Field(..., description="Working directory for the task")
    script_dir: Path = Field(..., description="Directory containing R scripts")
    output_dir: Path = Field(..., description="Directory for output files")
    logs_folder: Optional[Path] = Field(default=None, description="Directory for storing logs")
    chromosome_name: Optional[str] = Field(default=None, description="Chromosome name to process")
    num_cores: Optional[int] = Field(default=1, description="Number of cores to use")
    sample_id_prefix: Optional[str] = Field(default=None, description="Optional prefix for sample ID")
    output_file: Optional[str] = Field(default=None, description="Output filename for storing results")
    docker_image: str = Field(default="ebarea1981/methyl-it:latest", description="Docker image to use")
    r_script_name: str = Field(default="read_counts.R", description="R script name for ReadCounts task")
    r_script_args: Dict[str, Any] = Field(default_factory=dict, description="Additional arguments for the R script")
    cytosine_report_filename: Optional[str] = Field(default=None, description="Cytosine report filename")
    min_coverage: Optional[int] = Field(default=None, description="Minimum coverage threshold")
    chromosome_list: Optional[List[str]] = Field(default=None, description="List of chromosomes to process")

    def get_output_dir(self) -> Path:
        return self.output_dir


class PreprocessInput(BaseModel):
    sample_id: str
    working_dir: Path
    genomes_dir: Optional[Path] = None
    reference_file: Optional[str] = None
    output_dir: Optional[Path] = None
    logs_folder: Optional[Path] = None
    chromosome_list: Optional[List[str]] = None
    min_coverage: int = 4
    num_cores: int = 1
    tmp_dir: Optional[Path] = None
    split_by_context: bool = False
    fastq_files: List[str] = Field(..., description="List of FASTQ file paths")
    script_dir: Optional[Path] = Field(default=Path("scripts"), description="Directory containing R scripts")

    @model_validator(mode="before")
    @classmethod
    def validate_inputs(cls, data: Any) -> Any:
        working_dir = data.get("working_dir")
        if working_dir and not os.path.isdir(working_dir):
            os.makedirs(working_dir)
            print(f"Created working directory {working_dir}")
        output_dir = data.get("output_dir")
        if output_dir and not os.path.isdir(output_dir):
            os.makedirs(output_dir)
            print(f"Created output directory {output_dir}")
        logs_folder = data.get("logs_folder")
        if logs_folder and not os.path.isdir(logs_folder):
            os.makedirs(logs_folder)
            print(f"Created logs directory {logs_folder}")
        tmp_dir = data.get("tmp_dir")
        if tmp_dir and not os.path.isdir(tmp_dir):
            os.makedirs(tmp_dir)
            print(f"Created tmp directory {tmp_dir}")
        script_dir = data.get("script_dir", "scripts")
        if script_dir and not os.path.isdir(script_dir):
            os.makedirs(script_dir)
            print(f"Created script directory {script_dir}")
        return data

    def get_output_dir(self) -> Path:
        return self.output_dir if self.output_dir is not None else self.working_dir


# Base Step Class (Abstract)
class BaseStep(ABC, Generic[TInput, TOutput]):
    def __init__(self, storage_client: StorageClient):
        self.storage_client = storage_client

    async def pre_execute(self, input_data: TInput, parent_output: Optional[BaseStepResult] = None) -> None:
        print(f"Running pre-execute for {self.__class__.__name__}")
        # Download input_files to working_dir
        for input_file in input_data.input_files:
            local_path = os.path.join(input_data.working_dir, os.path.basename(input_file))
            self.storage_client.download_file(input_file, local_path)

    async def post_execute(self, input_data: TInput, output: TOutput) -> None:
        print(f"Running post-execute for {self.__class__.__name__}")
        output_dir = input_data.get_output_dir()
        # Upload intermediate_files
        for key, file_path in output.intermediate_files.items():
            dest_path = os.path.join(output_dir, f"{key}_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            self.storage_client.upload_file(file_path, dest_path)
        # Upload output_file if specified
        if output.output_file and output.output_path:
            dest_path = os.path.join(output_dir, f"{output.output_file}_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            self.storage_client.upload_file(str(output.output_path), dest_path)

    @abstractmethod
    async def execute(self, input_data: TInput) -> TOutput:
        pass


# Step Classes
class StepAlign(BaseStep[AlignParams, AlignResult]):
    async def pre_execute(self, input_data: AlignParams, parent_output: Optional[BaseStepResult] = None) -> None:
        print(f"Running pre-execute for {self.__class__.__name__}")
        # Download input_files (FASTQ files) and reference genome
        for input_file in input_data.input_files:
            local_path = os.path.join(input_data.working_dir, os.path.basename(input_file))
            self.storage_client.download_file(input_file, local_path)
        if input_data.genomes_dir and input_data.reference_file:
            reference = os.path.join(input_data.genomes_dir, input_data.reference_file)
            self.storage_client.download_file(str(reference),
                                              os.path.join(input_data.working_dir, input_data.reference_file))

    async def execute(self, input_data: AlignParams) -> AlignResult:
        print(f"Executing {self.__class__.__name__}")
        # Create dummy BAM file and intermediate SAM file
        output_bam = os.path.join(input_data.get_output_dir(), f"{input_data.sample_id}.bam")
        temp_sam = os.path.join(input_data.working_dir, "temp.sam")
        with open(output_bam, "w") as f:
            f.write("Dummy BAM content for alignment")
        with open(temp_sam, "w") as f:
            f.write("Dummy SAM content for intermediate alignment")
        return AlignResult(
            status="success",
            metrics={"reads_aligned": 1000},
            intermediate_files={"sam": temp_sam},
            output_file=f"{input_data.sample_id}.bam",
            output_path=Path(output_bam)
        )


class StepExtract(BaseStep[MethExtractionParams, MethExtractionResult]):
    async def pre_execute(self, input_data: MethExtractionParams,
                          parent_output: Optional[BaseStepResult] = None) -> None:
        print(f"Running pre-execute for {self.__class__.__name__}")
        # Download BAM file from parent_output and input_files
        if parent_output and parent_output.output_path:
            bam_file = os.path.join(input_data.working_dir, "input.bam")
            self.storage_client.download_file(str(parent_output.output_path), bam_file)
        for input_file in input_data.input_files:
            local_path = os.path.join(input_data.working_dir, os.path.basename(input_file))
            self.storage_client.download_file(input_file, local_path)
        # Download reference genome
        if input_data.genomes_dir and input_data.reference_file:
            reference = os.path.join(input_data.genomes_dir, input_data.reference_file)
            self.storage_client.download_file(str(reference),
                                              os.path.join(input_data.working_dir, input_data.reference_file))

    async def execute(self, input_data: MethExtractionParams) -> MethExtractionResult:
        print(f"Executing {self.__class__.__name__}")
        # Create dummy gzipped cytosine report and intermediate text file
        output_report = os.path.join(input_data.get_output_dir(), f"{input_data.sample_id}.cytosine_report.txt.gz")
        temp_txt = os.path.join(input_data.working_dir, "temp.txt")
        with open(temp_txt, "w") as f:
            f.write("Dummy text content for extraction")
        with open(output_report, "wb") as f_gz:
            with gzip.open(f_gz, "wt") as f:
                f.write("Dummy cytosine report content")
        return MethExtractionResult(
            status="success",
            metrics={"sites_processed": 500},
            intermediate_files={"temp": temp_txt},
            output_file=f"{input_data.sample_id}.cytosine_report.txt.gz",
            output_path=Path(output_report)
        )


class StepReadCounts(BaseStep[ReadCountsParams, ReadCountsResult]):
    async def pre_execute(self, input_data: ReadCountsParams, parent_output: Optional[BaseStepResult] = None) -> None:
        print(f"Running pre-execute for {self.__class__.__name__}")
        # Download cytosine report from parent_output and input_files
        if parent_output and parent_output.output_path:
            report_file = os.path.join(input_data.working_dir, "cytosine_report.txt.gz")
            self.storage_client.download_file(str(parent_output.output_path), report_file)
        for input_file in input_data.input_files:
            local_path = os.path.join(input_data.working_dir, os.path.basename(input_file))
            self.storage_client.download_file(input_file, local_path)

    async def execute(self, input_data: ReadCountsParams) -> ReadCountsResult:
        print(f"Executing {self.__class__.__name__} for chromosome {input_data.chromosome_name}")
        # Create dummy RData file and intermediate RDS file
        output_rdata = os.path.join(input_data.get_output_dir(),
                                    f"{input_data.sample_id}_reads_{input_data.chromosome_name}.RData")
        temp_rds = os.path.join(input_data.working_dir, "temp.rds")
        with open(output_rdata, "w") as f:
            f.write(f"Dummy RData content for {input_data.chromosome_name}")
        with open(temp_rds, "w") as f:
            f.write(f"Dummy RDS content for {input_data.chromosome_name}")
        return ReadCountsResult(
            status="success",
            metrics={"reads_counted": 200},
            intermediate_files={"temp": temp_rds},
            output_file=f"{input_data.sample_id}_reads_{input_data.chromosome_name}.RData",
            output_path=Path(output_rdata)
        )


# Reusable Durable Task Wrapper
async def run_step_task(
        step_class: Type[BaseStep],
        input_data: BaseModel,
        ctx: DurableContext,
        storage_client: StorageClient,
        parent_output: Optional[BaseStepResult] = None
) -> BaseStepResult:
    step = step_class(storage_client)
    await step.pre_execute(input_data, parent_output)
    result = await step.execute(input_data)
    await step.post_execute(input_data, result)
    return result


# Child Workflow: subflow_readcounts
subflow_readcounts = hatchet.workflow(name="ReadCountsSubflow", input_validator=ReadCountsParams)


@subflow_readcounts.durable_task(execution_timeout=timedelta(hours=1), retries=3)
async def subflow_readcounts_task(input: ReadCountsParams, ctx: DurableContext) -> ReadCountsResult:
    storage_client = LocalStorageClient()
    # No parent_output, as this is a standalone subflow
    return await run_step_task(StepReadCounts, input, ctx, storage_client)


# Main Workflow: flow_preprocess_sample
flow_preprocess_sample = hatchet.workflow(name="PreprocessSampleWorkflow", input_validator=PreprocessInput)


# Durable Tasks
@flow_preprocess_sample.durable_task(execution_timeout=timedelta(hours=1), retries=3)
async def step_align_task(input: PreprocessInput, ctx: DurableContext) -> AlignResult:
    storage_client = LocalStorageClient()
    # Create AlignParams from PreprocessInput
    align_params = AlignParams(
        sample_id=input.sample_id,
        working_dir=input.working_dir,
        genomes_dir=input.genomes_dir or Path("data/genomes"),  # Provide default if None
        reference_file=input.reference_file or "reference.fa",  # Provide default if None
        output_dir=input.output_dir,
        num_cores=input.num_cores,
        min_coverage=input.min_coverage,
        logs_folder=input.logs_folder,
        input_files=input.fastq_files
    )
    return await run_step_task(StepAlign, align_params, ctx, storage_client)


@flow_preprocess_sample.durable_task(parents=[step_align_task], execution_timeout=timedelta(hours=1), retries=3)
async def step_extract_task(input: PreprocessInput, ctx: DurableContext) -> MethExtractionResult:
    storage_client = LocalStorageClient()
    parent_output = ctx.task_output(step_align_task)
    # Create MethExtractionParams from PreprocessInput and parent output
    extract_params = MethExtractionParams(
        sample_id=input.sample_id,
        working_dir=input.working_dir,
        genomes_dir=input.genomes_dir or Path("data/genomes"),  # Provide default if None
        reference_file=input.reference_file or "reference.fa",  # Provide default if None
        output_dir=input.output_dir,
        num_cores=input.num_cores,
        min_coverage=input.min_coverage,
        logs_folder=input.logs_folder,
        chromosome_list=input.chromosome_list,
        input_files=[str(parent_output.output_path)] if parent_output and parent_output.output_path else [],
        minMAPQ=20,  # Default from MethExtractionParams
        minPhred=20,  # Default from MethExtractionParams
        output_prefix=None
    )
    return await run_step_task(StepExtract, extract_params, ctx, storage_client, parent_output)


@flow_preprocess_sample.durable_task(parents=[step_extract_task], execution_timeout=timedelta(hours=1), retries=3)
async def step_read_counts_task(input: PreprocessInput, ctx: DurableContext) -> BaseStepResult:
    storage_client = LocalStorageClient()
    parent_output = ctx.task_output(step_extract_task)

    # Prepare bulk run items for parallel execution
    read_counts_results = []
    if input.chromosome_list:
        bulk_run_items = [
            subflow_readcounts.create_bulk_run_item(
                input=ReadCountsParams(
                    sample_id=input.sample_id,
                    working_dir=input.working_dir,
                    script_dir=input.script_dir or Path("data/scripts"),
                    output_dir=input.output_dir or input.working_dir,
                    logs_folder=input.logs_folder,
                    chromosome_name=chrom,
                    num_cores=input.num_cores,
                    min_coverage=input.min_coverage,
                    input_files=[str(parent_output.output_path)] if parent_output and parent_output.output_path else [],
                    cytosine_report_filename=f"{input.sample_id}.cytosine_report.txt.gz"
                ),
                key=f"readcounts-{chrom}"
            )
            for chrom in input.chromosome_list
        ]
        ctx.log(f"Running {len(bulk_run_items)} child workflows in parallel")
        # Run child workflows in parallel using aio_run_many
        results = await subflow_readcounts.aio_run_many(bulk_run_items)
        for result in results:
            if isinstance(result, Exception):
                read_counts_results.append(ReadCountsResult(
                    status="error",
                    error_details=str(result),
                    metrics={},
                    intermediate_files={}
                ))
            else:
                result["status"] = "completed"
                read_counts_results.append(ReadCountsResult(**result))

    # Aggregate results
    status = "success" if all(r.status == "success" for r in read_counts_results) else "error"
    aggregated_intermediate_files = {}
    for i, result in enumerate(read_counts_results):
        aggregated_intermediate_files.update({f"{k}_chr{i + 1}": v for k, v in result.intermediate_files.items()})
    output_file = read_counts_results[0].output_file if read_counts_results else None
    output_path = read_counts_results[0].output_path if read_counts_results else None

    return BaseStepResult(
        status=status,
        metrics={"chromosomes_processed": len(read_counts_results)},
        intermediate_files=aggregated_intermediate_files,
        output_file=output_file,
        output_path=output_path,
        error_details="One or more chromosomes failed" if status == "error" else None
    )


# Main Execution
async def main():
    try:
        # Create temporary input files
        os.makedirs("data", exist_ok=True)
        for fastq_file in ["data/input_1.fastq", "data/input_2.fastq"]:
            with open(fastq_file, "w") as f:
                f.write(
                    "@SEQ_ID\nGATTTGGGGTTCAAAGCAGTATCGATCAAATAGTAAATCCATTTGTTCAACTCACAGTTT\n+\n!''*((((***+))%%%++)(%%%%).1***-+*''))**55CCF>>>>>>CCCCCCC65\n")
            print(f"Created temporary file {fastq_file}")

        input_data = PreprocessInput(
            sample_id="sample1",
            working_dir=Path("tmp/work_dir"),
            genomes_dir=Path("data/genomes"),
            reference_file="reference.fa",
            output_dir=Path("tmp/output"),
            logs_folder=Path("tmp/logs"),
            chromosome_list=["chr1", "chr2"],
            min_coverage=4,
            num_cores=1,
            tmp_dir=Path("tmp"),
            split_by_context=False,
            fastq_files=["data/input_1.fastq", "data/input_2.fastq"],
            script_dir=Path("data/scripts")
        )
        result = await flow_preprocess_sample.aio_run(input_data)
        print(f"Workflow result: {result}")
    except ValueError as e:
        print(f"Validation error: {e}")
    finally:
        # Clean working directory, preserving output directory
        working_dir = Path("tmp/work_dir")
        if working_dir.exists():
            shutil.rmtree(working_dir)
            working_dir.mkdir()
            print(f"Cleaned working directory {working_dir}")


if __name__ == "__main__":
    asyncio.run(main())