from datetime import datetime
import pandas as pd

from django.db.models import Count

from .models import Transaction, ItemCustomer, Client


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


def check_file(file):
    if not file.name.endswith(".csv"):
        return Result.fail("Invalid file type. Only .csv files are allowed")

    try:
        df = pd.read_csv(file)
        result = Result(desc=df)

    except ProcessDataError as e:
        error = str(e)
        return Result.fail(error)

    if not list(df.columns) == ['customer', 'item', 'total', 'quantity', 'date']:
        error = "Invalid data structure. Columns should be ['customer', 'item', 'total', 'quantity', 'date']"
        return Result.fail(error)

    return result


def check_date_format(dataframe):
    dataframe["date"] = pd.to_datetime(dataframe["date"])

    dataframe["date"] = dataframe["date"].apply(
        lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S.%f'))

    values_list = list(Transaction.objects.values_list("date", flat=True))

    values_list = pd.to_datetime(values_list, utc=False, errors='coerce')
    values_list = values_list.tz_localize(None)

    dataframe = dataframe[~dataframe["date"].isin(values_list)]

    return dataframe


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
