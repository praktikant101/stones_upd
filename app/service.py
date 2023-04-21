import pandas as pd
from .data_process import process_transactions, check_initial_data


def read_data(file):

    result = {"Status": "Fail", "Desc": ""}

    if not file.name.endswith(".csv"):
        error = "Invalid file type. Only .csv files are allowed"
        result["Desc"] = error
        return result

    try:
        df = pd.read_csv(file)
        result["Status"] = "OK"
        result["Desc"] = df

    except Exception as e:
        error = str(e)
        result["Desc"] = error
        return result

    if not list(df.columns) == ['customer', 'item', 'total', 'quantity', 'date']:
        error = "Invalid data structure. Columns should be ['customer', 'item', 'total', 'quantity', 'date']"
        result["Desc"] = error
        return result

    return result


def process_data(file):

    result = {"Status": "Fail", "Desc": ""}

    try:
        dataframe = read_data(file)

        if dataframe["Status"] == "Fail":
            result["Desc"] = dataframe["Desc"]
            return result

        dataframe = dataframe["Desc"]

        # checking if database for clients and items is empty
        # if there are objects, just proceeding further
        check_initial_data(dataframe)

        # saving Transaction objects
        # removing duplicates from dataframe if same objects are already in DB
        transactions_result = process_transactions(dataframe)
        if transactions_result["Status"] == "Fail":
            return transactions_result

    except Exception as e:
        error = str(e)
        result["Desc"] = "Failed to proceed data: " + error
        return result

    result["Status"] = "OK"
    result["Desc"] = "Data successfully processed"

    return result


