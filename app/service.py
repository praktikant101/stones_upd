import pandas as pd
from .data_process import process_transactions, process_clients, process_items, process_item_customer

from .models import Client, Item


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

    return result


def process_data(file):
    result = {"Status": "Fail", "Outcome": ""}

    try:
        dataframe = read_data(file)

        if dataframe["Status"] == "Fail":
            return dataframe["Outcome"]

        dataframe = dataframe["Outcome"]

        if not Client.objects.exists():
            clients_result = process_clients(dataframe)

            if clients_result["Status"] == "Fail":
                return clients_result

        if not Item.objects.exists():
            items_result = process_items(dataframe)

            if items_result["Status"] == "Fail":
                return items_result

        transactions_result = process_transactions(dataframe)
        if transactions_result["Status"] == "Fail":
            return transactions_result

        clients_result = process_clients(transactions_result["Outcome"])
        if clients_result["Status"] == "Fail":
            return clients_result

        items_result = process_items(transactions_result["Outcome"])
        if items_result["Status"] == "Fail":
            return items_result

        items_clients_result = process_item_customer(transactions_result["Outcome"])
        if items_clients_result["Status"] == "Fail":
            return items_clients_result

    except Exception as e:
        error = str(e)
        result["Outcome"] = "Failed to proceed data: " + error
        return result

    return result
