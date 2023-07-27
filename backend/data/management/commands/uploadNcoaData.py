from django.core.management.base import BaseCommand
from data.models import Client, ZipCode
import pandas as pd


class Command(BaseCommand):
    help = (
        "Upload NCOA data from a csv file to the database."
    )

    def add_arguments(self, parser):
        parser.add_argument("-f", "--file")

    def handle(self, *args, **options):
        file_name = options["file"]
        if file_name:
            df = pd.read_csv(file_name)
            for index, row in df.iterrows():
                print(index)
                try:
                    input_address = row['input_Street']
                    new_address = row['address']
                    if input_address != new_address:
                        zip_code = ZipCode.objects.get(
                            zip_code=row['input_Zip Code'])
                        client = Client.objects.filter(
                            address=input_address,
                            city=row['input_city'],
                            state=row['input_state'],
                            zip_code=zip_code
                        )
                        new_zip_code, _ = ZipCode.objects.get_or_create(
                            zip_code=row['zip'][:5])
                        client.update(
                            new_address=new_address,
                            new_city=row['city'],
                            new_state=row['st'],
                            new_zip_code=new_zip_code
                        )
                except Exception as e:
                    print(e)
