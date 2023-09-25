"""test of common argument parser modules"""

# import: datax in-house
from datax.utils.deployment_helper.entrypoint.parser import JobStatusArguments


def test_JobStatusArguments():
    """Test the `JobStatusArguments` class.

    Assertion statement:
        1. Validate if job status arguments are provided correctly.

    """
    args = JobStatusArguments().parse_args(
        [
            "--activate_etl_sts_log",
            "--parent_id",
            "46247807586690",
            "--job_name",
            "example_job",
            "--task_name",
            "TestModuleABCPipeline",
        ]
    )

    assert args.activate_etl_sts_log is True
    assert args.activate_feat_ml_sts_log is False
    assert args.parent_id == "46247807586690"
    assert args.run_id is None
    assert args.job_name == "example_job"
    assert args.task_name == "TestModuleABCPipeline"
