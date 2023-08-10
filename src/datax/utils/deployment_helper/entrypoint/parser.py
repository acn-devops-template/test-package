"""Common Argument Parser Module"""

# import: external
from tap import Tap


class JobStatusArguments(Tap):
    """Command-line arguments related to job status."""

    activate_job_status_log: bool = False
    """Activate log process to write job execution details into job log table."""

    parent_id: str = None
    """Databricks workflow ID."""

    parent_run_id: str = None
    """Databricks job ID."""

    run_id: str = None
    """Databricks task ID."""

    job_name: str = None
    """Workflow job name."""

    task_name: str = None
    """Task name within a workflow."""
