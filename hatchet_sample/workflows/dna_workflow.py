from hatchet_sdk import Context, WorkerLabelComparator
from hatchet_sdk.labels import DesiredWorkerLabel
from typing import Dict, Any
from pydantic import BaseModel
from hatchet_sample import hatchet

# Define input and output models using Pydantic for type safety
class WorkflowInput(BaseModel):
    data: Dict[str, Any]

class AlignOutput(BaseModel):
    result: str
    status: str

class ExtractOutput(BaseModel):
    result: Dict[str, Any]
    status: str

class ReadcountsOutput(BaseModel):
    result: Dict[str, Any]
    status: str
    data: Dict[str, Any]

# Define the DNA Workflow
dna_workflow = hatchet.workflow(name="dna-workflow", on_events=["dna:process"])

@dna_workflow.task(
    name="align",
    desired_worker_labels={
        'gpu': DesiredWorkerLabel(value='nvidia', required=True),
    }
)
def align(workflow_input: WorkflowInput, context: Context) -> AlignOutput:
    # This task runs on a worker with NVidia GPU
    print("******* Performing alignment on worker with NVidia GPU... *******")
    print(f"Workflow input: {workflow_input}")
    
    ## TODO: Implement alignment logic here
    
    print("******* Alignment complete *******")
    return AlignOutput(
        result="ALIGNMENT SUCCESS",
        status="success"
    )

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
def extract(workflow_input: WorkflowInput, context: Context) -> ExtractOutput:
    # This task runs on a worker with high RAM
    print("******* Performing extraction on worker with high RAM... *******")
    print(f"Workflow input: {workflow_input}")

    # Access output from "ALIGN" task
    align_result = context.task_output(align)
    print(f"Align result: {align_result.result}")

    ## TODO: Implement extraction logic here

    print("******* Extraction complete *******")
    return ExtractOutput(
        result={
            "extracted_data": "EXTRACTED SUCCESS"
        },
        status="success"
    )

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