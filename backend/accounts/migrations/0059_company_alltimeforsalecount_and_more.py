# Generated by Django 4.0.7 on 2023-01-12 17:26

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0058_alter_client_status_alter_homelisting_status'),
    ]

    operations = [
        migrations.AddField(
            model_name='company',
            name='allTimeForSaleCount',
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name='company',
            name='allTimeHomeSoldCount',
            field=models.IntegerField(default=0),
        ),
    ]
