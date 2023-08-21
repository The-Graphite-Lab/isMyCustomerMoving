# Generated by Django 3.2.8 on 2023-08-21 04:35

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0021_client_email'),
    ]

    operations = [
        migrations.CreateModel(
            name='ServiceTitanJob',
            fields=[
                ('id', models.CharField(max_length=100, primary_key=True, serialize=False)),
                ('job_type', models.CharField(max_length=100)),
                ('client', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='data.client')),
            ],
        ),
    ]
