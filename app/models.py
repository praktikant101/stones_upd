from django.db import models
from django.contrib.postgres.fields import ArrayField


class Client(models.Model):
    username = models.CharField(max_length=200, unique=True)
    spent_money = models.FloatField()
    gems = ArrayField(models.CharField(max_length=200), default=list, null=True)

    def __str__(self):
        return self.username


class Item(models.Model):
    name = models.CharField(max_length=200, unique=True)

    def __str__(self):
        return self.name


class ItemCustomer(models.Model):
    item = models.ForeignKey(Item, on_delete=models.CASCADE, related_name="items")
    client = models.ForeignKey(Client, on_delete=models.CASCADE, related_name="clients")


class Transaction(models.Model):
    client = models.ForeignKey(Client, on_delete=models.CASCADE)
    item = models.ForeignKey(Item, on_delete=models.CASCADE)
    price = models.FloatField()
    quantity = models.IntegerField()
    # datetime will serve as a unique identifier of the operation
    date = models.DateTimeField(unique=True)
