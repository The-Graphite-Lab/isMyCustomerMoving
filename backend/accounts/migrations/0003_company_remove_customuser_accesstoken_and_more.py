# Generated by Django 4.0.7 on 2022-09-12 05:17

from django.db import migrations, models
import django.db.models.deletion
import uuid


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0002_customuser_accesstoken'),
    ]

    operations = [
        migrations.CreateModel(
            name='Company',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False, unique=True)),
                ('name', models.CharField(choices=[('test', 'test'), ('kinetico_knoxville', 'kinetico_knoxville')], max_length=100)),
                ('accessToken', models.CharField(choices=[('test', 'test'), ('1qaz2wsx', '1qaz2wsx')], max_length=100)),
            ],
        ),
        migrations.RemoveField(
            model_name='customuser',
            name='accessToken',
        ),
        migrations.AlterField(
            model_name='customuser',
            name='company',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='accounts.company'),
        ),
    ]
