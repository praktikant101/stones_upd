import warnings

from .models import Client, Item, ItemCustomer, Transaction
from .data_check import check_date_format, check_gems

# avoiding unnecessary warnings that litter logs
warnings.filterwarnings("ignore", category=RuntimeWarning, message="DateTimeField .* received a naive datetime")


def heat_data(dataframe):
    if not Client.objects.exists():
        update_clients(dataframe)

    if not Item.objects.exists():
        process_items(dataframe)

def handling_gems(queryset):
    gems_filtered, gems = check_gems(queryset)

    customers_gems_update = []
    for element in gems_filtered:
        element.client.gems = gems
        customers_gems_update.append(element.client)

    Client.objects.bulk_update(customers_gems_update, ["gems"])


def update_clients(dataframe):
    new_customers = UserService.get_new_users(dataframe)
    existing_users = UserService.get_existing_users_with_updated_spent_money(dataframe)

    UserService.update_or_create_users(new_customers + existing_users)


def process_items(dataframe):
    item_values = Item.objects.values_list("name", flat=True)
    items = dataframe["item"].unique()
    items_new = []

    for item in items:
        if item in item_values:
            continue
        else:
            items_new.append(Item(name=item))

    Item.objects.bulk_create(items_new)


def process_item_customer(dataframe):
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


def process_transactions(dt):
    # validating date format and removing duplicates
    dataframe = check_date_format(dt)

    # saving clients and items into DB if they are not there yet
    for f in [update_clients, process_items, process_item_customer]:
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


class UserService:
    @classmethod
    def get_new_users(self, data):
        username_values = self.get_current_username_values()
        clients_prices = data.groupby("customer")["total"].agg(sum)

        new_customers = []

        for username, price in clients_prices.items():
            if username not in username_values:
                new_customers.append(Client(username=username, spent_money=price))

        return new_customers

    @classmethod
    def get_existing_users_with_updated_spent_money(self, data):
        username_values = self.get_current_username_values()
        clients_prices = data.groupby("customer")["total"].agg(sum)

        existing_users = []

        for username, price in clients_prices.items():
            if username in username_values:
                client = Client.objects.get(username=username)
                client.spent_money += price
                existing_users.append(client)

        return existing_users

    @classmethod
    def get_current_username_values(self):
        clients = self.get_current_users()
        username_values = clients.values_list("username", flat=True)
        return username_values

    @classmethod
    def update_or_create_users(self, users):
        for user in users:
            Client.objects.update_or_create(
                username=user.username,
                defaults={'spent_money': user.spent_money},
            )

    @classmethod
    def get_current_users(self):
        return Client.objects.all()
