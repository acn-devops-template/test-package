"""Common Argument Parser Module"""

# import: external
from tap import Tap


class JobStatusArguments(Tap):
    """Command-line arguments related to job status."""

    activate_etl_sts_log: bool = False
    """Activate log process to write run result into tbl_etl_sts_log."""

    activate_feat_ml_sts_log: bool = False
    """Activate log process to write run result into tbl_fp_sts_log."""

    parent_id: str = None
    """Databricks workflow ID."""

    parent_run_id: str = None
    """Databricks job ID."""

    run_id: str = None
    """Databricks task ID."""

    job_name: str = None
    """Workflow job name."""

    task_name: str = None
    """Job task name"""
