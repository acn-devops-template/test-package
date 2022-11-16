# import: standard
import subprocess


def test_dynamic_endpoint_caller_with_default_arg():

    # output = subprocess.run(['$python test_dynamic_endpoint.py'])
    output = exec(open("test_dynamic_endpoint.py -m test_dynamic_endpoint_main").read())

    print(output)

    assert output is True
