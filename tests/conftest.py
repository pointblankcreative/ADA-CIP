import pytest


@pytest.fixture
def sample_project_code():
    return "25013"


@pytest.fixture
def sample_project():
    return {
        "project_code": "25013",
        "project_name": "BCGEU Bargaining Escalation",
        "client_id": "bcgeu",
        "status": "active",
        "start_date": "2025-01-15",
        "end_date": "2025-04-30",
        "net_budget": 150000.00,
    }
