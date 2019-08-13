import sys
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

REQUIRED_PYTHON = "python3"


def main():
    system_major = sys.version_info.major
    if REQUIRED_PYTHON == "python":
        required_major = 2
    elif REQUIRED_PYTHON == "python3":
        required_major = 3
    else:
        raise ValueError("Unrecognized python interpreter: {}".format(
            REQUIRED_PYTHON))

    if system_major != required_major:
        raise TypeError(
            "This project requires Python {}. Found: Python {}".format(
                required_major, sys.version))
    else:
        print(">>> Development environment passes all tests!")


if __name__ == '__main__':
    
    client = MongoClient()

    try:
        main()
        client.admin.command('ismaster')
    except ConnectionFailure:
        print("Server not available. Please install or start MongoDB instance")
    except (ValueError, TypeError) as err:
        print(f'{err}')
