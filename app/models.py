from django.db import models


class Client(models.Model):
    username = models.CharField(max_length=200, unique=True)
    spent_money = models.FloatField()


class Item(models.Model):
    name = models.CharField(max_length=200, unique=True)


class StoneCustomer(models.Model):
    stone = models.ForeignKey(Item, on_delete=models.CASCADE)
    client = models.ForeignKey(Client, on_delete=models.CASCADE)


class Transaction(models.Model):
    client = models.ForeignKey(Client, on_delete=models.CASCADE)
    stone = models.ForeignKey(Item, on_delete=models.CASCADE)
    price = models.FloatField()
    quantity = models.IntegerField()
    # datetime will serve as a unique identifier of the operation
    date = models.DateTimeField(unique=True)

