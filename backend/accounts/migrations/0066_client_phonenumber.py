# Generated by Django 4.0.7 on 2023-01-15 16:53

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0065_alter_clientupdate_client'),
    ]

    operations = [
        migrations.AddField(
            model_name='client',
            name='phoneNumber',
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
    ]
