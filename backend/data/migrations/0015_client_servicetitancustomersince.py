# Generated by Django 3.2.8 on 2023-06-05 12:09

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0014_client_revenue'),
    ]

    operations = [
        migrations.AddField(
            model_name='client',
            name='serviceTitanCustomerSince',
            field=models.DateField(blank=True, null=True),
        ),
    ]
