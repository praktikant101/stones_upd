from .data_process import (
    process_transactions,
    check_initial_data,
)
from .data_check import check_file, Result, ProcessDataError


def process_data(file):
    try:
        dataframe = check_file(file)

        if dataframe.status == "Fail":
            return Result.fail("Invalid file type. Only .csv files are allowed")

        dataframe = dataframe.desc

        # checking if database for clients and items is empty
        # if there are objects, just proceeding further
        check_initial_data(dataframe)

        # saving Transaction objects
        # removing duplicates from dataframe if same objects are already in DB
        transactions_result = process_transactions(dataframe)
        if transactions_result.status == "Fail":
            return transactions_result

    except ProcessDataError as e:
        return Result.fail("Failed to proceed data: " + str(e))

    return Result.success()
