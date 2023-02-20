from django.db import models
import uuid

# Create your models here.
class Product(models.Model):
    id = models.UUIDField(primary_key=True, unique=True,
                          default=uuid.uuid4, editable=False)
    tier = models.CharField(max_length=50)
    timeFrame = models.CharField(max_length=50)
    pid = models.CharField(max_length=50)
    customerLimit = models.IntegerField(default=0)
    realEstateAddOn = models.BooleanField(default=False)


    def __str__(self):
        return self.tier