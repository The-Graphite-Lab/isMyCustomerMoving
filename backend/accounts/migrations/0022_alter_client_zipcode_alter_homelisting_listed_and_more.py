# Generated by Django 4.0.7 on 2022-09-20 15:19

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0021_alter_customuser_role_alter_homelisting_listed_and_more'),
    ]

    operations = [
        migrations.AlterField(
            model_name='client',
            name='zipCode',
            field=models.ForeignKey(default=11111, on_delete=django.db.models.deletion.CASCADE, to='accounts.zipcode'),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name='homelisting',
            name='listed',
            field=models.CharField(default='2022-09-20 15:19:16.195682', max_length=30),
        ),
        migrations.AlterField(
            model_name='zipcode',
            name='lastUpdated',
            field=models.DateField(default='2022-09-19'),
        ),
    ]
