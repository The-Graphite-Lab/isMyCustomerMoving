# Generated by Django 4.0.7 on 2023-01-15 02:16

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0062_remove_company_alltimeforsalecount_and_more'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='clientupdate',
            name='contactValChanged',
        ),
        migrations.AddField(
            model_name='clientupdate',
            name='contacted',
            field=models.BooleanField(blank=True, null=True),
        ),
    ]
