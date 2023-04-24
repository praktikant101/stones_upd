import warnings

from .models import Client, Item, ItemCustomer, Transaction
from .data_check import check_date_format, check_gems, check_clients, Result, ProcessDataError

# avoiding unnecessary warnings that litter logs
warnings.filterwarnings("ignore", category=RuntimeWarning, message="DateTimeField .* received a naive datetime")


def check_initial_data(dataframe):
    if not Client.objects.exists():

        clients_result = process_clients(dataframe)

        if clients_result.status == "Fail":
            return clients_result

    if not Item.objects.exists():
        items_result = process_items(dataframe)

        if items_result.status == "Fail":
            return items_result


def handling_gems(queryset):
    gems_filtered, gems = check_gems(queryset)

    customers_gems_update = []
    for element in gems_filtered:
        element.client.gems = gems
        customers_gems_update.append(element.client)

    Client.objects.bulk_update(customers_gems_update, ["gems"])


def process_clients(dataframe):
    clients = Client.objects.all()

    try:
        existing_customers, new_customers = check_clients(clients, dataframe)

        Client.objects.bulk_update(existing_customers, ["spent_money"])
        Client.objects.bulk_create(new_customers)

        return Result.success()

    except ProcessDataError as e:
        return Result.fail("Failed to proceed clients data: " + str(e))


def process_items(dataframe):
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

        return Result.success()

    except ProcessDataError as e:
        return Result.fail("Failed to proceed items data: " + str(e))


def process_item_customer(dataframe):
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

        return Result.success()

    except ProcessDataError as e:
        return Result.fail("Failed to proceed item_customer data: " + str(e))


def process_transactions(dt):

    try:
        # validating date format and removing duplicates
        dataframe = check_date_format(dt)

        if dataframe.status == "Fail":
            return dataframe

        dataframe = dataframe.desc

        # saving clients and items into DB if they are not there yet
        for f in [process_clients, process_items, process_item_customer]:
            outcome = f(dataframe)
            if outcome.status == "Fail":
                return outcome

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

        return Result.success()

    except ProcessDataError as e:
        error = str(e)
        return Result.fail("Failed to proceed transactions data: " + error)
