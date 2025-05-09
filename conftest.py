import pytest
from hatchet_sdk import Hatchet

@pytest.fixture(scope="session")
def hatchet() -> Hatchet:
    # Initialize Hatchet with necessary configuration
    return Hatchet(debug=True) 