# Generated by Django 4.0 on 2023-03-21 20:33

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("data", "0006_client_equipmentinstalleddate"),
    ]

    operations = [
        migrations.AddField(
            model_name="homelisting",
            name="city",
            field=models.CharField(blank=True, max_length=40, null=True),
        ),
        migrations.AddField(
            model_name="homelisting",
            name="state",
            field=models.CharField(blank=True, max_length=31, null=True),
        ),
    ]
