import pytest
##########################################################################################

def pytest_addoption(parser):
    parser.addoption("--db", action="store", default="default name")
    parser.addoption("--user", action="store", default="default name")
    parser.addoption("--pw", action="store", default="default pw")


def pytest_generate_tests(metafunc):
    # This is called for every test. Only get/set command line arguments
    # if the argument is specified in the list of test "fixturenames".
    #option_value = metafunc.config.option.name
    option_value = metafunc.config.option.db
    if 'db' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("db", [option_value])

    option_value2 = metafunc.config.option.user
    if 'user' in metafunc.fixturenames and option_value2 is not None:
        metafunc.parametrize("user", [option_value])

    option_value3 = metafunc.config.option.pw
    if 'pw' in metafunc.fixturenames and option_value3 is not None:
        metafunc.parametrize("pw", [option_value])