from django.db import models
import uuid
from datetime import datetime, timedelta

from accounts.models import Company

STATUS = [
    ('House For Sale','House For Sale'),
    ('House For Rent','House For Rent'),
    ('House Recently Sold (6)','House Recently Sold (6)'),
    ('Recently Sold (12)','Recently Sold (12)'),
    ('Taken Off Market', 'Taken Off Market'),
    ('No Change', 'No Change')
]

def zipTime():
    return (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

class ZipCode(models.Model):
    zipCode = models.CharField(max_length=5, primary_key=True, unique=True)
    lastUpdated = models.DateField(default=zipTime)

class Client(models.Model):
    id = models.UUIDField(primary_key=True, unique=True,
                          default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=100)
    address = models.CharField(max_length=100)
    zipCode = models.ForeignKey(ZipCode, on_delete=models.CASCADE)
    company = models.ForeignKey(Company, blank=True, null=True, on_delete=models.CASCADE, db_index=True)
    status = models.CharField(max_length=25, choices=STATUS, default='No Change', db_index=True)
    city = models.CharField(max_length=40, blank=True, null=True)
    state = models.CharField(max_length=31, blank=True, null=True)
    contacted = models.BooleanField(default=False)
    note = models.TextField(default="", blank=True, null=True)
    servTitanID = models.IntegerField(blank=True, null=True)
    phoneNumber = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        unique_together = ('company', 'name', 'address')

def formatToday():
    return datetime.today().strftime('%Y-%m-%d')

class ClientUpdate(models.Model):
    id = models.UUIDField(primary_key=True, unique=True,
                          default=uuid.uuid4, editable=False)
    client = models.ForeignKey(Client, on_delete=models.CASCADE, related_name='clientUpdates')
    date = models.DateField(default=formatToday)
    status = models.CharField(max_length=25, choices=STATUS, default='No Change', blank=True, null=True)
    listed = models.CharField(max_length=30, blank=True, null=True)
    note = models.TextField(default="", blank=True, null=True)
    contacted = models.BooleanField(blank=True, null=True)

class ScrapeResponse(models.Model):
    id = models.UUIDField(primary_key=True, unique=True,
                          default=uuid.uuid4, editable=False)
    date = models.DateField(default=formatToday)
    response = models.TextField(default="")
    zip = models.IntegerField(blank=True, null=True)
    status = models.CharField(max_length=25, choices=STATUS, default='No Change', blank=True, null=True)
    url = models.CharField(max_length=100, blank=True, null=True)

class HomeListing(models.Model):
    id = models.UUIDField(primary_key=True, unique=True,
                          default=uuid.uuid4, editable=False)
    zipCode = models.ForeignKey(ZipCode, blank=True, null=True, on_delete=models.SET_NULL)
    address = models.CharField(max_length=100)
    status = models.CharField(max_length=25, choices=STATUS, default='Off Market')
    listed = models.CharField(max_length=30, default=" ")
    ScrapeResponse = models.ForeignKey(ScrapeResponse, blank=True, null=True, on_delete=models.SET_NULL)

class Task(models.Model):
    id = models.UUIDField(primary_key=True, unique=True,
                          default=uuid.uuid4, editable=False)
    completed = models.BooleanField(default=False)
    deletedClients = models.IntegerField(default=0)