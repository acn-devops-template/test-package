""" path adjuster tests """

# import: standard
import unittest

# import: datax in-house
from datax.utils.deployment_helper.converter.path_adjuster import find_conf_path
from datax.utils.deployment_helper.converter.path_adjuster import replace_conf_reference

# import: external
import git


class TestFindConfPath(unittest.TestCase):
    """Test Class for testing find_conf_path.

    Class for testing find_conf_path.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    def test_find_conf_path(self) -> None:
        """Test find conf path using __file__."""
        find_resources_conf = find_conf_path(__file__, to_conf="resources/conf")
        git_repo = git.Repo(__file__, search_parent_directories=True)
        git_repo = git_repo.git.rev_parse("--show-toplevel")

        self.assertEqual(find_resources_conf, f"{git_repo}/tests/resources/conf")

    def test_find_conf_path_not_found(self) -> None:
        """Test find conf path for error, FileNotFoundError."""
        test_error_dict = {"file": __file__, "to_conf": "conf"}
        self.assertRaises(FileNotFoundError, find_conf_path, **test_error_dict)


class TestReplaceConfReference(unittest.TestCase):
    """Test Class for testing replace_conf_reference.

    Class for testing replace_conf_reference.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    def test_replace_conf_reference(self) -> None:
        """Test replacing conf reference."""

        conf_path = "/test/for_test"

        mock_dict = {
            "key_1": "smth",
            "key_2": {
                "sub_k1": "nothing",
                "sub_k2": "conf:/dir/file.txt",
                "sub_k3": None,
            },
            "key_3": "conf:/dir/sub_dir/file.json",
            "key_4": [1, 2],
            "key_5": 123,
        }

        expected_dict = {
            "key_1": "smth",
            "key_2": {
                "sub_k1": "nothing",
                "sub_k2": f"{conf_path}/dir/file.txt",
                "sub_k3": None,
            },
            "key_3": f"{conf_path}/dir/sub_dir/file.json",
            "key_4": [1, 2],
            "key_5": 123,
        }

        replace_conf_reference(mock_dict, conf_path)

        self.assertEqual(mock_dict, expected_dict)
