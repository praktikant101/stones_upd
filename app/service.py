from .data_process import (
    process_transactions,
    heat_data,
)
from .data_check import check_file

from stones_exceptions import StonesException

def process_data(file):
    try:
        dataframe = check_file(file)

        # checking if database for clients and items is empty
        # if there are objects, just proceeding further
        heat_data(dataframe)

        # saving Transaction objects
        # removing duplicates from dataframe if same objects are already in DB
        transactions_result = process_transactions(dataframe)
        if transactions_result.status == "Fail":
            return transactions_result

    except StonesException as e:
        return Result.fail("Failed to proceed data - Business Logic error: " + e.message)
    except Exception as e:
        return Result.fail("Failed to proceed data - Non functional error: " + str(e))

    return Result.success()

class ProcessDataError(BaseException):
    pass


class Result:
    def __init__(self, status="OK", desc="Data proceeded successfully"):
        self.status = status
        self.desc = desc

    @classmethod
    def fail(cls, error_message):
        result = cls(status="Fail", desc=error_message)
        return result

    @classmethod
    def success(cls):
        return cls()
