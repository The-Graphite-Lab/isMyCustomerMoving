from celery import shared_task
from datetime import datetime
from django.db import IntegrityError
from io import StringIO
import ast
import logging
import modal
from pandas import isna, read_csv


from .models import Client, HomeListing, ZipCode
from .utils import del_variables


@shared_task
def get_all_zipcodes(company, zip=None):
    """
    This task retrieves all the zip codes associated with a given company.
    """
    # Initialization of variables
    company_object, zip_code_objects, zip_codes, zips = "", "", "", ""
    try:
        print(f"Company: {company}")
        # Get distinct zip codes related to the company
        zip_codes = Client.objects.filter(company_id=company, active=True
                                          ).values_list(
                                              "zip_code", flat=True).distinct()
        print(f"Zip codes to process 1: {len(zip_codes)}")
        # Fetch the first 10 records in zip_codes
        first_10_records = zip_codes[:10]
        for record in first_10_records:
            print(f"Zip Code: {record.zip_code}, Last Updated: {record.last_updated}")
        
        print(f"Today's Date: {datetime.today().date()}")


        # Filter ZipCode objects and update their last_updated field
        zip_codes_to_update = ZipCode.objects.filter(
            zip_code__in=zip_codes,
            last_updated__lt=datetime.today().date()
        )

        print(f"Zip codes to process 2: {len(zip_codes_to_update)}")

        # Create a list of zip codes for further processing
        zips = list(zip_codes_to_update.order_by(
            "zip_code").values("zip_code"))
        print(f"Zip codes to process 3: {len(zips)}")

        zip_codes_to_update.update(last_updated=datetime.today().date())

    except Exception as e:
        if zip:
            zips = [{"zip_code": str(zip)}]
        else:
            logging.error(e)

    listing_types = ["for_sale", "sold"]
    zip_tuples = [(zip_code['zip_code'], listing_type)
                  for zip_code in zips for listing_type in listing_types]
    print(f"Zip tuples to process: {len(zip_tuples)}")
    f = modal.Function.lookup("imcm-scraper", "get_addresses")
    count = 0
    for results in f.starmap(zip_tuples):
        print(f"Processing {count} of {len(zip_tuples)}")
        count += 1
        if not results.empty:
            try:
                update_or_create_listing(results.to_csv(index=False))
            except Exception as e:
                logging.error(e)
                print(e)

    # send listings to zapier
    # days_to_run = [0]  # Only on Monday
    # current_day = datetime.now().weekday()

    # if current_day in days_to_run:
    #     send_zapier_recently_sold.delay(company)
    del_variables(
        [company_object, zip_code_objects, zip_codes, zips, company]
    )


def safe_assign(value, default=None):
    return default if isna(value) else value


def get_status(listing_type):
    mapping = {
        'FOR_SALE': 'House For Sale',
        'SOLD': 'House Recently Sold (6)',
        'PENDING': 'Pending'
        # Add other mappings here as required
    }
    return mapping.get(listing_type, 'Off Market')


def get_zipcode_instances(zip_codes):
    # Fetch existing zip codes from the database
    existing_zip_codes = ZipCode.objects.filter(zip_code__in=zip_codes)
    zip_code_map = {zc.zip_code: zc for zc in existing_zip_codes}

    # Create missing zip code instances
    missing_zip_codes = set(zip_codes) - set(zip_code_map.keys())
    for missing_zip in missing_zip_codes:
        zip_code_map[missing_zip], _ = ZipCode.objects.get_or_create(
            zip_code=missing_zip)

    return zip_code_map


@shared_task
def update_or_create_listing(df):
    df = read_csv(StringIO(df))
    df = df.dropna(subset=['street', 'state', 'city', 'zip_code'])
    df = df.drop_duplicates(
        subset=['street', 'unit', 'city', 'state'], keep='first')
    # Pre-fetch all relevant ZipCode instances
    zip_code_map = get_zipcode_instances(df['zip_code'].unique().tolist())

    df['full_address'] = df['street']
    # Create a full address field, only include unit if it's not '#' or 'nan'
    mask = df['unit'] != '#'
    df.loc[mask, 'full_address'] = df['street'] + ' ' + df['unit']
    possible_addresses = df['full_address'].tolist() + \
        df['street'].tolist()

    # Fetch all existing records that match the addresses in the dataframe
    existing_listings = HomeListing.objects.filter(
        address__in=possible_addresses,
        city__in=df['city'].tolist(),
        state__in=df['state'].tolist()
    ).values('address', 'city', 'state')

    existing_addresses = {(x['address'], x['city'], x['state'])
                          for x in existing_listings}

    to_update = []
    to_create = []
    for _, row in df.iterrows():
        try:
            address = row['street']
            unit = safe_assign(row['unit'])
            if unit:
                address = row['street'] + ' ' + unit
            city = row['city']
            state = row['state']
            status = get_status(row['status'])
            today = datetime.today().date().strftime('%Y-%m-%d')
            if status == 'House For Sale':
                listed = safe_assign(row.get('list_date', ''))
            else:
                listed = safe_assign(row.get('last_sold_date', ''))
            if not listed:
                listed = today
            tags = safe_assign(row.get('tags', []))
            if isinstance(tags, str):
                tags = tags.replace('_', ' ')
                tags = ast.literal_eval(tags)
            data = {
                # Get the ZipCode instance from the map
                'zip_code': zip_code_map[row['zip_code']],
                'address': address,
                'status': status,
                'price': safe_assign(row.get('list_price',
                                             row.get('sold_price', 0))),
                'year_built': safe_assign(row.get('year_built', 1900)),
                'city': city,
                'state': state,
                'bedrooms': safe_assign(row.get('beds', 0)),
                'bathrooms': safe_assign(row.get('full_baths', 0)),
                'sqft': safe_assign(row.get('sqft', 0)),
                'lot_sqft': safe_assign(row.get('lot_sqft', 0)),
                'latitude': safe_assign(row.get('latitude', 0)),
                'longitude': safe_assign(row.get('longitude', 0)),
                'url': row['property_url'],
                'garage': safe_assign(row.get('parking_garage', 0)),
                'description': safe_assign(row.get('description', '')),
                'listed': listed,
                'tags': tags

            }

            listing_obj = HomeListing(**data)

            if address is not None and address != "None None None" \
                    and city is not None and state is not None:
                if (address, city, state) in existing_addresses:
                    to_update.append(listing_obj)
                else:
                    to_create.append(listing_obj)
        except Exception as e:
            logging.error(e)
            print(e)

    try:
        if to_update:
            HomeListing.objects.bulk_update(to_update, [
                'zip_code', 'status', 'price', 'year_built', 'bedrooms',
                'bathrooms', 'sqft', 'lot_sqft', 'latitude', 'longitude',
                'url'  # Add other fields as required
            ])
            print(f"Updated {len(to_update)} listings")
    except IntegrityError as e:
        print(f"UPDATE: {e}")
        print(len(to_update))
        successful_counts = 0
        for update in to_update:
            try:
                HomeListing.objects.filter(
                    address=update.address,
                    city=update.city,
                    state=update.state
                ).update(
                    zip_code=update.zip_code,
                    status=update.status,
                    price=update.price,
                    year_built=update.year_built,
                    bedrooms=update.bedrooms,
                    bathrooms=update.bathrooms,
                    sqft=update.sqft,
                    lot_sqft=update.lot_sqft,
                    latitude=update.latitude,
                    longitude=update.longitude,
                    url=update.url
                )
                successful_counts += 1
            except IntegrityError as e:
                print("ERROR:", e)
        print(f"Updated {successful_counts} listings")
    try:
        if to_create:
            HomeListing.objects.bulk_create(to_create)
            print(f"Created {len(to_create)} new listings")
    except IntegrityError as e:
        print(f"CREATE: {e}")
        print(len(to_create))
        successful_counts = 0
        for create in to_create:
            try:
                HomeListing.objects.create(
                    zip_code=create.zip_code,
                    status=create.status,
                    price=create.price,
                    year_built=create.year_built,
                    bedrooms=create.bedrooms,
                    bathrooms=create.bathrooms,
                    sqft=create.sqft,
                    lot_sqft=create.lot_sqft,
                    latitude=create.latitude,
                    longitude=create.longitude,
                    url=create.url
                )
                successful_counts += 1
            except IntegrityError as e:
                print(e)
        print(f"Created {successful_counts} new listings")
