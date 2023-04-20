import pandas as pd
from datetime import datetime

from .models import Client, Item, StoneCustomer, Transaction


def process_transactions(dataframe):
    # removing duplicates by checking uniqueness according to datetime

    dataframe["date"] = pd.to_datetime(dataframe["date"])

    dataframe["date"] = dataframe["date"].apply(
        lambda x: datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S.%f'))

    values_list = Transaction.objects.values_list("date", flat=True)

    dataframe = dataframe[~dataframe["date"].isin(values_list)]

    new_transactions = dataframe.to_dict("records")



