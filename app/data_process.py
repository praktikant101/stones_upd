import pandas as pd
from datetime import datetime

from .models import Client, Item, StoneCustomer, Transaction

import warnings

warnings.filterwarnings("ignore", category=RuntimeWarning, message="DateTimeField .* received a naive datetime")


def process_transactions(dataframe):
    # removing duplicates by checking uniqueness according to datetime

    result = {"Status": "Fail", "Outcome": ""}

    try:
        dataframe["date"] = pd.to_datetime(dataframe["date"])

        dataframe["date"] = dataframe["date"].apply(
            lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S.%f'))

        values_list = list(Transaction.objects.values_list("date", flat=True))

        values_list = pd.to_datetime(values_list, utc=False, errors='coerce')
        values_list = values_list.tz_localize(None)

        dataframe = dataframe[~dataframe["date"].isin(values_list)]

        transactions_to_create = []

        for row in dataframe.itertuples(index=True, name='Pandas'):
            transactions_to_create.append(Transaction(client=Client.objects.get(username=getattr(row, "customer")),
                                                      stone=Item.objects.get(name=getattr(row, "item")),
                                                      price=getattr(row, "total"),
                                                      quantity=getattr(row, "quantity"),
                                                      date=getattr(row, "date")))

        Transaction.objects.bulk_create(transactions_to_create)

        # saving filtered dataframe to return to work on it further while processing clients and items
        result["Status"] = "OK"
        result["Outcome"] = dataframe

    except Exception as e:
        error = str(e)
        result["Outcome"] = "Failed to proceed transactions data: " + error
        return result

    return result


def process_clients(dataframe):
    result = {"Status": "Fail", "Outcome": ""}

    try:
        username_values = Client.objects.values_list("username", flat=True)
        clients_prices = dataframe.groupby("customer")["total"].agg(sum)

        new_customers = []
        existing_customers = []

        for username, price in clients_prices.items():
            if username in username_values:
                client = Client.objects.get(username=username)
                client.spent_money += price
                existing_customers.append(client)
            else:
                new_customers.append(Client(username=username, spent_money=price))

        Client.objects.bulk_update(existing_customers, ["spent_money"])
        Client.objects.bulk_create(new_customers)

        result["Status"] = "OK"
        result["Outcome"] = "Data successfully processed"

        return result

    except Exception as e:
        result["Outcome"] = "Failed to proceed clients data: " + str(e)
        return result


def process_items(dataframe):
    result = {"Status": "Fail", "Outcome": ""}

    try:
        item_values = Item.objects.values_list("name", flat=True)
        items = dataframe["item"].unique()
        items_new = []

        for item in items:
            if item in item_values:
                continue
            else:
                items_new.append(Item(name=item))

        Item.objects.bulk_create(items_new)

        result["Status"] = "OK"
        result["Outcome"] = "Data successfully processed"

        return result

    except Exception as e:
        result["Outcome"] = "Failed to proceed items data: " + str(e)
        return result


def process_item_customer(dataframe):
    result = {"Status": "Fail", "Outcome": ""}

    try:
        stone_customer_values = StoneCustomer.objects.values_list("stone__name", "client__username")
        stone_customer_new = []

        for stone, clients in dataframe.groupby("item")["customer"].agg(set).items():
            for client in clients:
                if (stone, client) in stone_customer_values:
                    continue
                else:
                    stone_customer_new.append(StoneCustomer(stone=Item.objects.get(name=stone),
                                                            client=Client.objects.get(username=client)))

        StoneCustomer.objects.bulk_create(stone_customer_new)

        result["Status"] = "OK"
        result["Outcome"] = "Data successfully processed"

        return result

    except Exception as e:
        result["Outcome"] = "Failed to proceed item customer data: " + str(e)
        return result
