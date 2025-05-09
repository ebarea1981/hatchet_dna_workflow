from hatchet_sample.workflows import WorkflowInput, trigger_workflow


def main() -> None:
     # Example input data
    sample_input = {"sample_id": "DNA123", "sequence": "ATCG..."}
    trigger_workflow(sample_input)


if __name__ == "__main__":
    main()