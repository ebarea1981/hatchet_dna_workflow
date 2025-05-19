from abc import ABC, abstractmethod
from asyncio.log import logger
from typing import TypeVar, Generic, Type, Any
import os
from datetime import datetime
from hatchet_sdk import Hatchet, DurableContext
from pydantic import BaseModel, model_validator
import asyncio

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

# Base Input and Result Classes (Pydantic for Hatchet compatibility)
class BaseStepInput(BaseModel):
    input_path: str
    working_dir: str

    @model_validator(mode="before")
    @classmethod
    def validate_inputs(cls, data: Any) -> Any:
        # Validate input_path exists
        input_path = data.get("input_path")
        if not input_path or not os.path.exists(input_path):
            raise ValueError(f"Input path {input_path} does not exist")
        # Create working_dir if it doesn't exist
        working_dir = data.get("working_dir")
        if working_dir and not os.path.isdir(working_dir):
            os.makedirs(working_dir)
            print(f"Created working directory {working_dir}")
        return data

class BaseStepResult(BaseModel):
    output_path: str

# Step1 and Step2 Inputs
class Step1Input(BaseStepInput):
    param1: str

class Step2Input(BaseStepInput):
    pass  # No additional fields needed for Step2

# Base Step Class (Abstract)
class BaseStep(ABC, Generic[TInput, TOutput]):
    def __init__(self, storage_client: StorageClient):
        self.storage_client = storage_client

    async def pre_execute(self, input_data: TInput) -> None:
        print(f"Running pre-execute for {self.__class__.__name__}")
        # Removed input_data.validate() call, as validation is now handled by Pydantic
        local_path = os.path.join(input_data.working_dir, "input_file")
        self.storage_client.download_file(input_data.input_path, local_path)

    async def post_execute(self, input_data: TInput, output: TOutput) -> None:
        print(f"Running post-execute for {self.__class__.__name__}")
        output_path = os.path.join(input_data.working_dir, f"output_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        self.storage_client.upload_file(output.output_path, output_path)

    @abstractmethod
    async def execute(self, input_data: TInput) -> TOutput:
        pass

# Step Classes
# Note: @step_execution_decorator is not used because it conflicts with Hatchet's
# durable task model. run_step_task handles the pre/execute/post sequence instead.
class Step1(BaseStep[Step1Input, BaseStepResult]):
    async def execute(self, input_data: Step1Input) -> BaseStepResult:
        print(f"Executing Step1 with param1: {input_data.param1}")
        input_file = os.path.join(input_data.working_dir, "input_file")
        output_file = os.path.join(input_data.working_dir, "step1_output.txt")
        with open(input_file, "r") as f:
            content = f.read()
        with open(output_file, "w") as f:
            f.write(f"Step1 processed: {content} with {input_data.param1}")
        return BaseStepResult(output_path=output_file)

class Step2(BaseStep[Step2Input, BaseStepResult]):
    async def execute(self, input_data: Step2Input) -> BaseStepResult:
        print("Executing Step2")
        input_file = os.path.join(input_data.working_dir, "input_file")
        output_file = os.path.join(input_data.working_dir, "step2_output.txt")
        with open(input_file, "r") as f:
            content = f.read()
        with open(output_file, "w") as f:
            f.write(f"Step2 processed: {content}")
        return BaseStepResult(output_path=output_file)

# Reusable Durable Task Wrapper
# Replaces @step_execution_decorator, providing the pre/execute/post sequence
# in a Hatchet-compatible, async way.
async def run_step_task(step_class: Type[BaseStep], input_data: BaseModel, ctx: DurableContext, storage_client: StorageClient) -> BaseStepResult:
    step = step_class(storage_client)
    await step.pre_execute(input_data)
    result = await step.execute(input_data)
    await step.post_execute(input_data, result)
    return result

# Workflow Definition
step_workflow = hatchet.workflow(name="StepWorkflow")

# Durable Tasks
@step_workflow.durable_task()
async def step1_durable_task(input: Step1Input, ctx: DurableContext) -> BaseStepResult:
    storage_client = LocalStorageClient()  # In production, inject via config
    return await run_step_task(Step1, input, ctx, storage_client)

@step_workflow.durable_task(parents=[step1_durable_task])
async def step2_durable_task(input: Step2Input, ctx: DurableContext) -> BaseStepResult:
    storage_client = LocalStorageClient()  # In production, inject via config
    # Access Step1's output
    step1_result = ctx.task_output(step1_durable_task)
    print(f"Step1 output: {step1_result}")
    return await run_step_task(Step2, input, ctx, storage_client)

def start_worker():
    """Start a Hatchet worker to process workflows."""
    logger.info("Starting Hatchet worker 'test-worker' with 6 slots")
    worker = hatchet.worker("step-worker", workflows=[step_workflow])
    worker_task = asyncio.create_task(worker.start())
    return worker_task

# Example Usage
async def main():
    # Trigger workflow
    input_data = Step1Input(
        input_path="data/input.txt",
        working_dir="tmp/work_dir/step1",
        param1="example"
    )
    step2_input = Step2Input(
        input_path="data/input2.txt",  # Could use step1_result.output_path
        working_dir="tmp/work_dir/step2"
    )

    try:
        result = await step_workflow.aio_run(input_data)
        logger.info(f"Batch workflow result: {result}")
    except Exception as e:
        logger.error(f"Workflow execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())