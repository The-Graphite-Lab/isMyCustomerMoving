# Generated by Django 4.0.7 on 2022-09-12 21:38

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0007_alter_customuser_company'),
    ]

    operations = [
        migrations.RenameModel(
            old_name='Clients',
            new_name='Client',
        ),
        migrations.RemoveField(
            model_name='clientlist',
            name='company',
        ),
    ]
