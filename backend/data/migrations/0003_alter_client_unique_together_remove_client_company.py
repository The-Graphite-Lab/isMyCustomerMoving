# Generated by Django 4.0 on 2023-03-01 14:36

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0002_auto_20230301_1413'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='client',
            unique_together=set(),
        ),
        migrations.RemoveField(
            model_name='client',
            name='company',
        ),
    ]
