# Generated by Django 3.2.8 on 2023-05-25 02:23

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [        
        ("accounts", "0003_auto_20230512_0431"),
    ]

    operations = [
        migrations.AddField(
            model_name="company",
            name="serviceTitanAppVersion",
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
