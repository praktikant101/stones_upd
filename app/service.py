import pandas as pd
from .data_process import process_transactions


def read_data(file):

    result = {"Status": "Fail", "Outcome": ""}

    if not file.name.endswith(".csv"):
        error = "Invalid file type. Only .csv files are allowed"
        result["Outcome"] = error
        return result

    try:
        df = pd.read_csv(file)
        result["Status"] = "OK"
        result["Outcome"] = df

    except Exception as e:
        error = str(e)
        result["Outcome"] = error
        return result

    if not list(df.columns) == ['customer', 'item', 'total', 'quantity', 'date']:
        error = "Invalid data structure. Columns should be ['customer', 'item', 'total', 'quantity', 'date']"
        result["Outcome"] = error
        return result

    df.columns = ['client', 'item', 'price', 'quantity', 'date']

    return result


def process_data(file):

    dataframe = read_data(file)

    if dataframe["Status"] == "Fail":
        return dataframe["Outcome"]
    process_transactions(dataframe["Outcome"])
