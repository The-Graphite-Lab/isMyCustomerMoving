# Generated by Django 4.1 on 2023-01-30 19:28

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("accounts", "0077_scraperesponse_homelisting_scraperesponse"),
    ]

    operations = [
        migrations.AddField(
            model_name="company",
            name="recentlySoldPurchased",
            field=models.BooleanField(default=False),
        ),
    ]
