from datetime import datetime
import pandas as pd

from django.db.models import Count

from .models import Transaction, ItemCustomer, Client

from stones_exceptions import StonesException


def check_file(file):
    if not file.name.endswith(".csv"):
        raise InvalidFileType

    df = pd.read_csv(file)

    if not (list(df.columns) == ['customer', 'item', 'total', 'quantity', 'date']):
        raise ProcessDataError

    return df


def check_date_format(dataframe):
    dataframe["date"] = pd.to_datetime(dataframe["date"])

    dataframe["date"] = dataframe["date"].apply(
        lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S.%f'))

    values_list = list(Transaction.objects.values_list("date", flat=True))

    values_list = pd.to_datetime(values_list, utc=False, errors='coerce')
    values_list = values_list.tz_localize(None)

    dataframe = dataframe[~dataframe["date"].isin(values_list)]



def check_gems(queryset):
    top_clients = queryset.order_by("-spent_money")[:5]

    # items bought by top 5 customers
    items_top_clients = ItemCustomer.objects.filter(client__in=top_clients)

    # and at least by 2 of them
    items_top_clients = items_top_clients.values("item__name", "item").annotate(count=Count("item__name")).filter(
        count__gt=1)

    gems = []
    gems_id = []

    for item in items_top_clients:
        gems.append(item["item__name"])
        gems_id.append(item["item"])

    # customers that have gems assigned
    gems_filtered = ItemCustomer.objects.filter(item__in=gems_id).distinct("client")

    return gems_filtered, gems


def check_clients(queryset, dataframe):
    username_values = queryset.values_list("username", flat=True)
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

    return existing_customers, new_customers


class InvalidFileType(StonesException):
    def __init__(self, message="Invalid file type. Only .csv files are allowed") -> None:
        super().__init__(message)


class ProcessDataError(StonesException):
    def __init__(self,
                 message="Invalid data structure. Columns should be ['customer', 'item', 'total', 'quantity', 'date']") -> None:
        super().__init__(message)
