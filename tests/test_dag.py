import time
import pytest

from hatchet_sample.workflows.dna_workflow import dna_workflow
from hatchet_sdk import Hatchet

@pytest.mark.asyncio(loop_scope="session")
async def test_run() -> None:
    result = await dna_workflow.aio_run()
    # assert result["extract"]["extract_result"] == "extracted_result"
    # assert result["align"]["align_result"] == "aligned_result"
