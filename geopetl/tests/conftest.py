import pytest
##########################################################################################

def pytest_addoption(parser):
    parser.addoption("--db", action="store", default="default name")
    parser.addoption("--user", action="store", default="default name")
    parser.addoption("--pw", action="store", default="default pw")

    parser.addoption("--host", action="store", default="default name")
    parser.addoption("--port", action="store", default="default name")
    parser.addoption("--service_name", action="store", default="default name")
    parser.addoption("--schema", action="store", default="default name")

def pytest_generate_tests(metafunc):
    # This is called for every test. Only get/set command line arguments
    # if the argument is specified in the list of test "fixturenames".
    #option_value = metafunc.config.option.name
    option_value = metafunc.config.option.db
    if 'db' in metafunc.fixturenames and option_value is not None:
        metafunc.parametrize("db", [option_value])

    option_value2 = metafunc.config.option.user
    if 'user' in metafunc.fixturenames and option_value2 is not None:
        metafunc.parametrize("user", [option_value2])

    option_value3 = metafunc.config.option.pw
    if 'pw' in metafunc.fixturenames and option_value3 is not None:
        metafunc.parametrize("pw", [option_value3])

##########
    option_value4 = metafunc.config.option.host
    if 'host' in metafunc.fixturenames and option_value4 is not None:
        metafunc.parametrize("host", [option_value4])

    option_value5 = metafunc.config.option.port
    if 'port' in metafunc.fixturenames and option_value5 is not None:
        metafunc.parametrize("port", [option_value5])

    option_value6 = metafunc.config.option.service_name
    if 'service_name' in metafunc.fixturenames and option_value6 is not None:
        metafunc.parametrize("service_name", [option_value6])

    option_value7 = metafunc.config.option.schema
    if 'schema' in metafunc.fixturenames and option_value7 is not None:
        metafunc.parametrize("schema", [option_value7])
