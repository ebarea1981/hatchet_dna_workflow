from hatchet_sdk import Hatchet, Context, WorkerLabelComparator
from hatchet_sdk.labels import DesiredWorkerLabel
from typing import Dict, Any, Annotated
from pydantic import BaseModel
import threading
import time

from hatchet_sample import hatchet

# Define input and output models using Pydantic for type safety
class WorkflowInput(BaseModel):
    data: Dict[str, Any]

class AlignInput(BaseModel):
    data: Dict[str, Any]

class ExtractInput(BaseModel):
    align_input: Dict[str, Any]
    align_result: Dict[str, Any]

class WorkflowOutput(BaseModel):
    align_result: Dict[str, Any]
    extract_result: Dict[str, Any]

# Define the DNA Workflow
dna_workflow = hatchet.workflow(name="dna-workflow", on_events=["dna:process"])

@dna_workflow.task(
    name="align",
    desired_worker_labels={
        'gpu': DesiredWorkerLabel(value='nvidia', required=True),
    }
)
async def align(align_input: AlignInput, context: Context) -> Annotated[Dict[str, Any], "align_result"]:
    # This task runs on a worker with NVidia GPU
    print("******* Performing alignment on worker with NVidia GPU... *******")
    
    # input_data = context.input.get("data", {})
    # align_input = AlignInput(data=input_data)
    # Implement alignment logic here
    
    print("******* Alignment complete *******")
    return {"aligned_data": "aligned_result", "status": "success"}

@dna_workflow.task(
    name="extract",
    parents=[align],
    desired_worker_labels={
        'memory': DesiredWorkerLabel(
            value=1024,  # Assuming 1024 represents high memory, adjust as needed
            required=True,
            comparator=WorkerLabelComparator.GREATER_THAN_OR_EQUAL
        ),
    }
)
async def extract(extract_input: ExtractInput, context: Context) -> Annotated[Dict[str, Any], "extract_result"]:
    # This task runs on a worker with high RAM
    print("******* Performing extraction on worker with high RAM... *******")
    workflow_input = context.input.get("data", {})
    print(f"Workflow input: {workflow_input}")
    
    # align_result = context.get_previous_step_output("align", {})
    # print(f"Align result: {align_result}")
    
    # extract_input = ExtractInput(align_input=workflow_input, align_result=align_result)
    # Implement extraction logic here

    print("******* Extraction complete *******")
    return {"extracted_data": "extracted_result", "status": "success"}

# Example function to trigger the workflow
def trigger_workflow(input_data: Dict[str, Any]):
    workflow_input = WorkflowInput(data=input_data)
    # Trigger the event to start the workflow using the correct method
    try:
        result = hatchet.event.push("dna:process", workflow_input.model_dump())
        print(f"Triggered DNA workflow with input: {input_data}, result: {result}")
        
        # If you need to fetch the result, handle potential errors
        if hasattr(result, 'result'):
            try:
                run_result = result.result()
                print(f"Workflow run result: {run_result}")
            except ValueError as e:
                print(f"Error fetching workflow result: {e}")
    except Exception as e:
        print(f"Error triggering workflow: {e}")

if __name__ == "__main__":
    
    # Example input data
    sample_input = {"sample_id": "DNA123", "sequence": "ATCG..."}
    trigger_workflow(sample_input)