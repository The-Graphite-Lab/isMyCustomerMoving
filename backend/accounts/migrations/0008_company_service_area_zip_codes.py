# Generated by Django 3.2.8 on 2023-08-15 04:41

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data', '0020_auto_20230722_0048'),
        ('accounts', '0007_alter_company_product'),
    ]

    operations = [
        migrations.AddField(
            model_name='company',
            name='service_area_zip_codes',
            field=models.ManyToManyField(blank=True, related_name='service_area_zip_codes', to='data.ZipCode'),
        ),
    ]
