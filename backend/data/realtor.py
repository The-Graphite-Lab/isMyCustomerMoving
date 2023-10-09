from celery import shared_task
from datetime import datetime
from io import StringIO
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
        # Get distinct zip codes related to the company
        zip_codes = Client.objects.filter(company_id=company, active=True
                                          ).values_list(
                                              "zip_code", flat=True).distinct()
        print(f"Zip codes to process 1: {len(zip_codes)}")

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

    site_name = "realtor.com"
    listing_types = ["for_sale", "sold"]
    zip_tuples = [(zip_code['zip_code'], site_name, listing_type)
                  for zip_code in zips for listing_type in listing_types]
    print(f"Zip tuples to process: {len(zip_tuples)}")
    f = modal.Function.lookup("imcm-scraper", "get_addresses")
    count = 0
    for results in f.starmap(zip_tuples):
        print(f"Processing {count} of {len(zip_tuples)}")
        count += 1
        if not results.empty:
            # update_or_create_listing.delay(results.to_csv(index=False))
            # testing without celery to see if it gets rid of errors
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
        'for_sale': 'House For Sale',
        'sold': 'House Recently Sold (6)',
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
    df = df.dropna(subset=['address_one', 'state', 'city', 'zip_code'])
    df = df.drop_duplicates(
        subset=['address_one', 'address_two', 'city', 'state'], keep='first')
    # Pre-fetch all relevant ZipCode instances
    zip_code_map = get_zipcode_instances(df['zip_code'].unique().tolist())

    df['full_address'] = df['address_one']
    mask = df['address_two'] != '#'
    df.loc[mask, 'full_address'] = df['address_one'] + ' ' + df['address_two']

    # Fetch all existing records that match the addresses in the dataframe
    existing_listings = HomeListing.objects.filter(
        address__in=df['full_address'].tolist(),
        city__in=df['city'].tolist(),
        state__in=df['state'].tolist()
    ).values('address', 'city', 'state')

    existing_addresses = {(x['address'], x['city'], x['state'])
                          for x in existing_listings}

    to_update = []
    to_create = []

    for _, row in df.iterrows():
        try:
            address = row['address_one']
            if row['address_two'] != '#':
                address += f' {row["address_two"]}'
            city = row['city']
            state = row['state']
            status = get_status(row['listing_type'])
            list_date = safe_assign(row.get(
                'list_date', datetime.now().date().strftime('%Y-%m-%d')))
            if list_date:
                # See if list date is less than 6 months from today
                list_date = list_date.split('T')[0]
                if status == "House Recently Sold (6)":
                    list_date_str = datetime.strptime(list_date, '%Y-%m-%d')
                    if (datetime.now().date() - list_date_str.date()).days > 180:
                        status = 'Off Market'
            else:
                continue

            data = {
                # Get the ZipCode instance from the map
                'zip_code': zip_code_map[row['zip_code']],
                'address': address,
                'status': status,
                'price': safe_assign(row.get('price_min', row.get('price_max', 0))),
                'year_built': safe_assign(row.get('year_built', 1900)),
                'city': city,
                'state': state,
                'bedrooms': safe_assign(row.get('beds_min', row.get('beds_max', 0))),
                'bathrooms': safe_assign(row.get('baths_min', row.get('baths_max', 0))),
                'sqft': safe_assign(row['sqft_min']),
                'lot_sqft': safe_assign(row.get('lot_area_value', 0)),
                'latitude': safe_assign(row.get('latitude', 0)),
                'longitude': safe_assign(row.get('longitude', 0)),
                'url': row['property_url'],
                'garage': safe_assign(row.get('garage', 0)),
                'description': safe_assign(row.get('description', '')),
                'listed': list_date,


            }

            listing_obj = HomeListing(**data)

            if (address, city, state) in existing_addresses:
                to_update.append(listing_obj)
            else:
                to_create.append(listing_obj)
        except Exception as e:
            logging.error(e)
            print(e)

    if to_update:
        HomeListing.objects.bulk_update(to_update, [
            'zip_code', 'status', 'price', 'year_built', 'bedrooms',
            'bathrooms', 'sqft', 'lot_sqft', 'latitude', 'longitude',
            'url'  # Add other fields as required
        ])
        print(f"Updated {len(to_update)} listings")

    if to_create:
        HomeListing.objects.bulk_create(to_create)
        print(f"Created {len(to_create)} new listings")
