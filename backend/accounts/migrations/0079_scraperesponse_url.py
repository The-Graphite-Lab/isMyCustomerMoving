# Generated by Django 4.1 on 2023-01-31 15:01

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("accounts", "0078_company_recentlysoldpurchased"),
    ]

    operations = [
        migrations.AddField(
            model_name="scraperesponse",
            name="url",
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
    ]
