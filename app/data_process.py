import pandas as pd
from datetime import datetime
import warnings

from django.db.models import Count

from .models import Client, Item, ItemCustomer, Transaction
from .secondary_service import check_date_format, check_gems, check_clients

# avoiding unnecessary warnings that litter logs
warnings.filterwarnings("ignore", category=RuntimeWarning, message="DateTimeField .* received a naive datetime")


def process_transactions(dataframe):
    # removing duplicates by checking the uniqueness of transactions according to datetime

    result = {"Status": "Fail", "Desc": ""}

    try:
        # saving clients and items into DB if they are not there yet
        for f in [process_clients, process_items, process_item_customer]:
            outcome = f(dataframe)
            if outcome["Status"] == "Fail":
                return outcome

        # validating date format and removing duplicates
        dataframe = check_date_format(dataframe)

        transactions_to_create = []

        for row in dataframe.itertuples(index=True, name='Pandas'):
            transactions_to_create.append(Transaction(client=Client.objects.get(username=getattr(row, "customer")),
                                                      item=Item.objects.get(name=getattr(row, "item")),
                                                      price=getattr(row, "total"),
                                                      quantity=getattr(row, "quantity"),
                                                      date=getattr(row, "date")))

        Transaction.objects.bulk_create(transactions_to_create)

        # updating gems for clients
        clients = Client.objects.all()
        handling_gems(clients)

        result["Status"] = "OK"
        result["Desc"] = "Data successfully processed"

    except Exception as e:
        error = str(e)
        result["Desc"] = "Failed to proceed transactions data: " + error
        return result

    return result


def handling_gems(queryset):

    gems_filtered, gems = check_gems(queryset)

    customers_gems_update = []
    for element in gems_filtered:
        element.client.gems = gems
        customers_gems_update.append(element.client)

    Client.objects.bulk_update(customers_gems_update, ["gems"])


def process_clients(dataframe):
    result = {"Status": "Fail", "Desc": ""}

    clients = Client.objects.all()

    try:
        existing_customers, new_customers = check_clients(clients, dataframe)

        Client.objects.bulk_update(existing_customers, ["spent_money"])
        Client.objects.bulk_create(new_customers)

        result["Status"] = "OK"
        result["Desc"] = "Data successfully processed"

        return result

    except Exception as e:
        result["Desc"] = "Failed to proceed clients data: " + str(e)
        return result


def process_items(dataframe):
    result = {"Status": "Fail", "Desc": ""}

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
        result["Desc"] = "Data successfully processed"

        return result

    except Exception as e:
        result["Desc"] = "Failed to proceed items data: " + str(e)
        return result


def process_item_customer(dataframe):
    result = {"Status": "Fail", "Desc": ""}

    try:
        item_customer_values = ItemCustomer.objects.values_list("item__name", "client__username")
        item_customer_new = []

        for item, clients in dataframe.groupby("item")["customer"].agg(set).items():
            for client in clients:
                if (item, client) in item_customer_values:
                    continue
                else:
                    item_customer_new.append(ItemCustomer(item=Item.objects.get(name=item),
                                                          client=Client.objects.get(username=client)))

        ItemCustomer.objects.bulk_create(item_customer_new)

        result["Status"] = "OK"
        result["Desc"] = "Data successfully processed"

        return result

    except Exception as e:
        result["Desc"] = "Failed to proceed item customer data: " + str(e)
        return result


def check_initial_data(dataframe):
    if not Client.objects.exists():

        clients_result = process_clients(dataframe)

        if clients_result["Status"] == "Fail":
            return clients_result

    if not Item.objects.exists():
        items_result = process_items(dataframe)

        if items_result["Status"] == "Fail":
            return items_result
