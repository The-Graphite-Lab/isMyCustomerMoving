# Generated by Django 4.0.7 on 2022-10-11 07:28

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0027_alter_homelisting_listed'),
    ]

    operations = [
        migrations.AlterField(
            model_name='homelisting',
            name='listed',
            field=models.CharField(default='2022-10-11 07:28:50.851542', max_length=30),
        ),
    ]
