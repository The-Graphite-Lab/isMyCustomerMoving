import importlib
from re import sub
from accounts.models import Company, CustomUser
from config import settings
from .models import (
    Client,
    ZipCode,
    HomeListing,
    ClientUpdate,
    Task,
    SavedFilter,
)
from .serializers import ZapierClientSerializer, HomeListingSerializer

from celery import shared_task
from datetime import datetime, timedelta, date
import gc
import json
import logging
import requests
import traceback
from defusedxml.ElementTree import fromstring

from django.db.models import Q
from django.template.loader import get_template
from django.core.mail import EmailMessage, send_mail


def del_variables(vars):
    for var in vars:
        try:
            del var
        except NameError:
            pass
    gc.collect()


def find_client_count(subscription_product):
    if (
        subscription_product.amount == 150
        and subscription_product.interval == "month"
    ) or (
        subscription_product.amount == 1650
        and subscription_product.interval == "year"
    ):
        ceiling = 5000
    elif (
        subscription_product.amount == 250
        and subscription_product.interval == "month"
    ) or (
        subscription_product.amount == 2750
        and subscription_product.interval == "year"
    ):
        ceiling = 10000
    elif (
        subscription_product.amount == 400
        and subscription_product.interval == "month"
    ) or (
        subscription_product.amount == 4400
        and subscription_product.interval == "year"
    ):
        ceiling = 20000
    elif (
        subscription_product.amount == 1500
        and subscription_product.interval == "month"
    ) or (
        subscription_product.amount == 16500
        and subscription_product.interval == "year"
    ):
        ceiling = 150000
    elif (
        subscription_product.amount == 5000
        and subscription_product.interval == "month"
    ) or (
        subscription_product.amount == 55000
        and subscription_product.interval == "year"
    ):
        ceiling = 500000
    else:
        ceiling = 100000
    return ceiling


def find_clients_to_delete(client_count, subscription_type):
    ceiling = find_client_count(subscription_type)
    if client_count > ceiling:
        return client_count - ceiling
    else:
        return 0


def reactivate_clients(company_id):
    company = Company.objects.get(id=company_id)
    clients = Client.objects.filter(company=company)
    client_ceiling = find_client_count(company.product.product.name)
    if client_ceiling > clients.count():
        clients.update(active=True)
    else:
        to_reactive_count = (
            client_ceiling - clients.filter(active=True).count()
        )
        if to_reactive_count > 0:
            clients.filter(active=False).order_by("id")[
                :to_reactive_count
            ].update(active=True)


@shared_task
def delete_extra_clients(company_id, task_id=None):
    """
    Delete extra clients based on the company's subscription limit.

    :param company_id: ID of the company
    :param task_id: ID of the task (optional)
    """
    try:
        company = Company.objects.get(id=company_id)
        clients = Client.objects.filter(company=company, active=True)
        deleted_clients = find_clients_to_delete(
            clients.count(), company.product
        )

        if deleted_clients > 0:
            Client.objects.filter(
                id__in=list(
                    clients.values_list("id", flat=True)[:deleted_clients]
                )
            ).update(active=False)

        #     admins = CustomUser.objects.filter(
        #         company=company, status="admin"
        #     )
        #     mail_subject = "IMCM Clients Deleted"
        #     message_plain = (
        #         "Your company has exceeded the number of clients..."
        #     )
        #     message_html = get_template("clientsDeleted.html").render(
        #         {"deleted_clients": deleted_clients}
        #     )

        #     for admin in admins:
        #         if "@test.com" not in admin.email:
        #             send_mail(
        #                 subject=mail_subject,
        #                 message=message_plain,
        #                 from_email=settings.EMAIL_HOST_USER,
        #                 recipient_list=[admin.email],
        #                 html_message=message_html,
        #                 fail_silently=False,
        #             )
    except Exception as e:
        logging.error(e)
        deleted_clients = 0

    if task_id:
        task = Task.objects.get(id=task_id)
        task.deleted_clients = deleted_clients
        task.completed = True
        task.save()


def parse_streets(street):
    """
    Parses street names into abbreviated forms.

    :param street: Street name
    :return: Abbreviated street name
    """
    conversions = {
        "Alley": "Aly",
        "Avenue": "Ave",
        "Boulevard": "Blvd",
        "Circle": "Cir",
        "Court": "Crt",
        "Cove": "Cv",
        "Canyon": "Cnyn",
        "Drive": "Dr",
        "Expressway": "Expy",
        "Highway": "Hwy",
        "Lane": "Ln",
        "Parkway": "Pkwy",
        "Place": "Pl",
        "Pike": "Pk",
        "Point": "Pt",
        "Road": "Rd",
        "Square": "Sq",
        "Street": "St",
        "Terrace": "Ter",
        "Trail": "Trl",
        "South": "S",
        "North": "N",
        "West": "W",
        "East": "E",
        "Northeast": "NE",
        "Northwest": "NW",
        "Southeast": "SE",
        "Southwest": "SW",
        "Ne": "NE",
        "Nw": "NW",
        "Sw": "SW",
        "Se": "SE",
    }
    for word in street.split():
        if word in conversions:
            street = street.replace(word, conversions[word])
    return street


def format_zip(zip_code):
    """
    Formats the zip code.

    :param zip_code: Zip code
    :return: Formatted zip code
    """
    if zip_code is None:
        return None

    try:
        if isinstance(zip_code, float):
            zip_code = int(zip_code)
        if isinstance(zip_code, str):
            zip_code = zip_code.replace(" ", "")
            zip_code = zip_code.split("-")[0]
        if 500 < int(zip_code) < 99951:
            zip_code = str(zip_code).zfill(5)
        return zip_code
    except ValueError:
        return False


@shared_task
def save_service_area_list(zip_codes, company_id):
    company = Company.objects.get(id=company_id)
    company.service_area_zip_codes.set([])
    for zip in zip_codes:
        zip = zip.get("Zip_Code", "")
        if zip:
            zip_code = format_zip(zip)
            if zip_code:
                if int(zip_code) < 500 or int(zip_code) > 99951:
                    continue
                zip_code_obj, _ = ZipCode.objects.get_or_create(
                    zip_code=str(zip_code)
                )
                company.service_area_zip_codes.add(zip_code_obj)


@shared_task
def save_client_list(clients, company_id, task=None):
    """
    Saves a list of clients to the database.

    :param clients: List of clients
    :param company_id: ID of the company
    :param task: Task instance (optional)
    """
    bad_streets = [
        "none",
        "null",
        "na",
        "n/a",
        "tbd",
        ".",
        "unk",
        "unknown",
        "no address listed",
        "no address",
        "cmo",
    ]
    clients_to_add = []
    company = Company.objects.get(id=company_id)

    for i, client in enumerate(clients):

        try:
            is_service_titan = "active" in client
            is_hubspot = "hs_object_id" in client

            if (
                is_service_titan and client["active"]
            ) or not is_service_titan:
                if is_hubspot:
                    name = client["firstname"] + " " + client["lastname"]
                else:
                    name = client["name"]
                if is_service_titan:
                    street = parse_streets(
                        client["address"]["street"].title()
                    )
                    zip_code = format_zip(client["address"]["zip"])
                    city = client["address"]["city"]
                    state = client["address"]["state"]
                else:
                    street = parse_streets(client["address"].title())
                    zip_code = format_zip(client["zip code"])
                    city = client["city"]
                    state = client["state"]

                if street.lower() in bad_streets or "tbd" in street.lower():
                    continue

                if zip_code is False or int(zip_code) < 500 or int(zip_code) > 99951:
                    zip_code_obj = ZipCode.objects.get_or_create(
                        zip_code="00000")[0]
                else:
                    zip_code_obj = ZipCode.objects.get_or_create(
                        zip_code=str(zip_code)
                    )[0]
                if len(name) > 100:
                    name = name[:100]
                if is_service_titan:
                    clients_to_add.append(
                        Client(
                            address=street,
                            zip_code=zip_code_obj,
                            city=city,
                            state=state,
                            name=name,
                            company=company,
                            serv_titan_id=client["customerId"],
                        )
                    )
                elif is_hubspot:
                    phone_number = (
                        sub("[^0-9]", "", client["phone"])

                    )
                    created_date = datetime.fromisoformat(
                        client["created"].rstrip('Z')).date()

                    clients_to_add.append(
                        Client(
                            address=street,
                            zip_code=zip_code_obj,
                            city=city,
                            state=state,
                            name=name,
                            company=company,
                            hubspot_id=client["hs_object_id"],
                            email=client["email"],
                            phone_number=phone_number,
                            customer_since=created_date
                        )
                    )
                else:
                    if i % 100 == 0 and i != 0:
                        Client.objects.bulk_create(
                            clients_to_add, ignore_conflicts=True
                        )
                        clients_to_add = []
                        logging.info(f"Saving {i} out of {len(clients)}")

                    phone_number = (
                        sub("[^0-9]", "", client["phone number"])
                        if "phone number" in client
                        else ""
                    )
                    clients_to_add.append(
                        Client(
                            address=street,
                            zip_code=zip_code_obj,
                            city=city,
                            state=state,
                            name=name,
                            company=company,
                            phone_number=phone_number,
                            email=client["email"] if "email" in client else "",
                        )
                    )
        except Exception as e:
            logging.error("create error")
            logging.error(e)
            logging.error(client)

    Client.objects.bulk_create(clients_to_add, ignore_conflicts=True)

    if task:
        delete_extra_clients.delay(company_id, task)
        clients_to_verify = list(Client.objects.filter(
            company=company, old_address=None
        ).values_list("id", flat=True))
        verify_address(clients_to_verify)
        auto_update.delay(company_id)

    del clients_to_add, clients, company, company_id, bad_streets


@shared_task
def update_client_list(numbers):
    phone_numbers, clients = "", ""
    phone_numbers = {}
    for number in numbers:
        try:
            if number.get("phoneSettings") is not None:
                phone_numbers[number["customerId"]] = number[
                    "phoneSettings"
                ].get("phoneNumber")
        except Exception as e:
            logging.error(f"update error {e}")
            continue
    clients = Client.objects.filter(
        serv_titan_id__in=list(phone_numbers.keys())
    )
    for client in clients:
        client.phone_number = phone_numbers[client.serv_titan_id]
        client.save()
    del_variables([phone_numbers, clients, numbers])


def find_clients_to_update(zip_code, company_id, status):

    company = Company.objects.get(id=company_id)
    zip_code_object = ZipCode.objects.get(zip_code=zip_code)

    listed_addresses = HomeListing.objects.filter(
        zip_code=zip_code_object, status=status
    ).values("address")

    previous_listed = Client.objects.filter(
        company=company,
        zip_code=zip_code_object,
        status=status,
        active=True,
        error_flag=False,
    )

    clients_to_update = Client.objects.filter(
        company=company,
        address__in=listed_addresses,
        zip_code=zip_code_object,
        active=True,
        error_flag=False,
    )

    newly_listed = clients_to_update.difference(previous_listed)
    del_variables([listed_addresses, previous_listed,
                  company, zip_code_object])

    return clients_to_update, newly_listed


def check_if_needs_update(client_id, status):
    client = Client.objects.get(id=client_id)
    listing_updates = ClientUpdate.objects.filter(
        client=client,
        status__in=["House For Sale", "House Recently Sold (6)"],
    )
    update = True

    for listing_update in listing_updates:
        if (
            listing_update.listed
            > HomeListing.objects.get(
                address=client.address, status=status
            ).listed
        ):
            update = False
    return update


@shared_task
def update_status(zip_code, company_id, status):
    """
    Update the status of listings based on the provided zip code and status.

    :param zip_code: The zip code of the listings to be updated.
    :param company_id: The ID of the company.
    :param status: The status to be set for the listings.
    """
    try:
        company = Company.objects.get(id=company_id)
        zip_code_object = ZipCode.objects.get(zip_code=zip_code)
    except Exception as e:
        logging.error(
            f"ERROR during updateStatus: {e} with zip_code {zip_code}"
        )
        return

    clients_to_update, newly_listed = find_clients_to_update(
        zip_code, company_id, status)

    scrapfly_count = 0
    updated_clients = []
    for to_list in newly_listed:

        to_update = check_if_needs_update(to_list.id, status)
        if to_update:
            home_listing = HomeListing.objects.get(
                address=to_list.address, status=status
            )
            scrapfly_count += 1
            to_list.status = status
            to_list.price = home_listing.price
            to_list.year_built = home_listing.year_built
            to_list.housing_type = home_listing.housing_type
            to_list.bedrooms = home_listing.bedrooms
            to_list.bathrooms = home_listing.bathrooms
            to_list.sqft = home_listing.sqft
            to_list.lot_sqft = home_listing.lot_sqft
            updated_clients.append(to_list)
            # to_list.tag.add(*home_listing.tag.all())

            zapier_url = (
                company.zapier_for_sale
                if status == "House For Sale"
                else company.zapier_sold
            )
            if zapier_url:
                try:
                    serializer = ZapierClientSerializer(to_list)
                    requests.post(
                        zapier_url, data=serializer.data, timeout=10
                    )
                except Exception as e:
                    logging.error(e)

        try:
            listing = HomeListing.objects.filter(
                zip_code=zip_code_object,
                address=to_list.address,
                status=status,
            )
            ClientUpdate.objects.get_or_create(
                client=to_list, status=status, listed=listing[0].listed
            )
        except Exception as e:
            logging.error(f"Cant find listing to list {e}")

    Client.objects.bulk_update(updated_clients, [
        "status", "price", "year_built", "housing_type", "bedrooms", "bathrooms"
    ])

    clients_to_update = [
        client
        for client in clients_to_update.values_list(
            "serv_titan_id", flat=True
        )
        if client
    ]

    if clients_to_update:
        update_service_titan_clients.delay(
            clients_to_update, company.id, status
        )


@shared_task
def update_clients_statuses(company_id=None):
    """
    Update the statuses of clients in all companies or a specific company.

    :param company_id: The ID of the specific company.
    If None, update for all companies.
    """
    try:
        companies = (
            Company.objects.filter(id=company_id)
            if company_id
            else Company.objects.all()
        )

        for company in companies:
            if company.product.id != "price_1MhxfPAkLES5P4qQbu8O45xy":
                zip_codes = (
                    Client.objects.filter(company=company, active=True)
                    .values("zip_code")
                    .distinct()
                    .order_by("zip_code")
                )

                for zip_code in zip_codes:
                    update_status.delay(
                        zip_code["zip_code"], company.id, "House For Sale"
                    )
                    update_status.delay(
                        zip_code["zip_code"],
                        company.id,
                        "House Recently Sold (6)",
                    )

    except Exception as e:
        logging.error(
            f"""ERROR during update_clients_statuses: {e} with company {company}"""
        )
        logging.error(traceback.format_exc())


@shared_task
def send_daily_email(company_id=None):
    (
        companies,
        company,
        emails,
        subject,
        for_sale_customers,
        sold_customers,
        message,
        email,
        msg,
    ) = ("", "", "", "", "", "", "", "", "")
    if company_id:
        companies = Company.objects.filter(id=company_id)
    else:
        companies = Company.objects.all()
    for company in companies:
        try:
            if company.product.id != "price_1MhxfPAkLES5P4qQbu8O45xy":
                emails = list(
                    CustomUser.objects.filter(company=company).values_list(
                        "email"
                    )
                )
                subject = "Did Your Customers Move?"

                for_sale_customers = (
                    Client.objects.filter(
                        company=company, status="House For Sale", active=True
                    )
                    .exclude(contacted=True)
                    .count()
                )
                sold_customers = (
                    Client.objects.filter(
                        company=company,
                        status="House Recently Sold (6)",
                        active=True,
                    )
                    .exclude(contacted=True)
                    .count()
                )
                message = get_template("dailyEmail.html").render(
                    {"forSale": for_sale_customers, "sold": sold_customers}
                )

                if for_sale_customers > 0 or sold_customers > 0:
                    for email in emails:
                        email = email[0]
                        msg = EmailMessage(
                            subject,
                            message,
                            settings.EMAIL_HOST_USER,
                            [email],
                        )
                        msg.content_subtype = "html"
                        msg.send()
        except Exception as e:
            logging.error(
                f"ERROR during send_daily_email: {e} with company {company}"
            )
            logging.error(traceback.format_exc())
    # if not company_id:
    #     HomeListing.objects.all().delete()
    ZipCode.objects.filter(
        lastUpdated__lt=datetime.today() - timedelta(days=3)
    ).delete()
    del_variables(
        [
            companies,
            company,
            emails,
            subject,
            for_sale_customers,
            sold_customers,
            message,
            email,
            msg,
        ]
    )


@shared_task
def auto_update(company_id=None, zip=None):
    from .realtor import get_all_zipcodes

    company = ""
    if company_id:
        try:
            company = Company.objects.get(id=company_id)
        except Exception as e:
            logging.error(f"Company does not exist {e}")
            return
        get_all_zipcodes(company_id)
        del_variables([company_id, company])
    elif zip:
        try:
            ZipCode.objects.get_or_create(zip_code=zip)
        except Exception as e:
            logging.error(f"Zip does not exist {e}")
            return
        get_all_zipcodes("", zip=zip)
    else:
        company, companies = "", ""
        companies = Company.objects.all()
        for company in companies:
            try:
                if company.product.id != "price_1MhxfPAkLES5P4qQbu8O45xy":
                    get_all_zipcodes(company.id)
                else:
                    logging.error("free tier")
            except Exception as e:
                logging.error(f"Auto Update Error: {e}")
                logging.error(
                    f"Auto Update: {company.product} {company.name}"
                )
        del_variables([company, companies])


def get_service_titan_access_token(company):
    company = Company.objects.get(id=company)
    if company.service_titan_app_version == 2:
        app_key = settings.ST_APP_KEY_2
    else:
        app_key = settings.ST_APP_KEY

    url = "https://auth.servicetitan.io/connect/token"

    payload = (
        f"grant_type=client_credentials&"
        f"client_id={company.client_id}&client_secret={company.client_secret}"
    )
    headers = {
        "ST-App-Key": app_key,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    response = requests.post(url, headers=headers, data=payload, timeout=5)
    response_data = response.json()
    access_token = response_data["access_token"]
    header = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "ST-App-Key": app_key,
    }

    return header


def process_client_tags(client_id):
    """
    Process tag removal for the given client.

    Parameters:
    client_id (str): ID of the client.
    """
    try:
        client = CustomUser.objects.get(id=client_id)
        headers = get_service_titan_access_token(client.company.id)
        company = client.company
        tag_ids = [
            str(company.service_titan_for_sale_tag_id),
            str(company.service_titan_recently_sold_tag_id),
            str(company.service_titan_for_sale_contacted_tag_id),
            str(company.service_titan_recently_sold_contacted_tag_id),
        ]

        payload = {
            "customerIds": [str(client.serv_titan_id)],
            "tagTypeIds": tag_ids,
        }
        handle_tag_deletion_request(payload, headers, client.company)

    except Exception as e:
        logging.error(e)


def determine_tag_type(company, status):
    """
    Determine the tag type based on the status.

    Parameters:
    company (object): Company object.
    status (str): Status of the property.

    Returns:
    list: List containing the tag type.
    """
    if status == "House For Sale":
        return [str(company.service_titan_for_sale_tag_id)]
    elif status == "House Recently Sold (6)":
        return [str(company.service_titan_recently_sold_tag_id)]


def handle_tag_deletion_request(
    payload, headers, company, client_subset=None
):
    """
    Send a tag deletion request to Service Titan API.

    Parameters:
    payload (dict): Payload for the request.
    headers (dict): Headers for the request.
    company (object): Company object.
    client_subset (list, optional): Subset of client IDs.

    Returns:
    response (object): Response from the Service Titan API.
    """
    base_url = "https://api.servicetitan.io/"
    response = requests.delete(
        f"{base_url}crm/v2/tenant/{str(company.tenant_id)}/tags",
        headers=headers,
        json=payload,
        timeout=10,
    )
    if response.status_code != 200:
        resp = response.json()
        error = (
            resp["title"]
            .replace("(", "")
            .replace(")", "")
            .replace(",", "")
            .replace(".", "")
            .split()
        )

        for word in error:
            if (
                word.isdigit()
                and client_subset
                and int(word) in client_subset
            ):
                client_subset.remove(int(word))

        if client_subset:
            payload = {
                "customerIds": client_subset,
                "tagTypeIds": payload["tagTypeIds"],
            }
            response = requests.delete(
                f"{base_url}crm/v2/tenant/{str(company.tenant_id)}/tags",
                headers=headers,
                json=payload,
                timeout=10,
            )

    return response


def handle_tag_addition_request(payload, headers, company, for_sale):
    """
    Send a tag addition request to Service Titan API.

    Parameters:
    payload (dict): Payload for the request.
    headers (dict): Headers for the request.
    company (object): Company object.
    for_sale (list): List of client IDs for sale.

    Returns:
    response (object): Response from the Service Titan API.
    """
    base_url = "https://api.servicetitan.io/"
    response = requests.put(
        f"{base_url}crm/v2/tenant/{str(company.tenant_id)}/tags",
        headers=headers,
        json=payload,
        timeout=10,
    )
    if response.status_code != 200:
        resp = response.json()
        error = (
            resp["title"]
            .replace("(", "")
            .replace(")", "")
            .replace(",", "")
            .replace(".", "")
            .split()
        )

        for word in error:
            if word.isdigit() and int(word) in for_sale:
                for_sale.remove(int(word))

        if for_sale:
            payload = {
                "customerIds": for_sale,
                "tagTypeIds": payload["tagTypeIds"],
            }
            response = requests.put(
                f"{base_url}crm/v2/tenant/{str(company.tenant_id)}/tags",
                headers=headers,
                json=payload,
                timeout=10,
            )

    return response


@shared_task
def update_service_titan_clients(clients_to_update, company, status):
    """
    Update Service Titan client tags.

    Parameters:
    for_sale (list): List of IDs of clients for sale.
    company (str): ID of the company.
    status (str): Status of the property.
    """
    response, payload, tag_type = "", "", ""
    try:
        company = Company.objects.get(id=company)
        headers = get_service_titan_access_token(company.id)
        tag_ids = [
            company.service_titan_for_sale_tag_id,
            company.service_titan_recently_sold_tag_id
        ]

        tag_ids = [str(tag_id) for tag_id in tag_ids if tag_id]

        if clients_to_update and tag_ids:
            tag_type = determine_tag_type(company, status)

            if status == "House Recently Sold (6)":
                for_sale_to_remove = clients_to_update
                payload = {
                    "customerIds": for_sale_to_remove,
                    "tagTypeIds": [
                        str(company.service_titan_for_sale_tag_id)
                    ],
                }
                response = handle_tag_deletion_request(
                    payload, headers, company, for_sale_to_remove
                )

                if response and response.status_code != 200:
                    logging.error(response.json())

            payload = {"customerIds": clients_to_update,
                       "tagTypeIds": tag_type}
            response = handle_tag_addition_request(
                payload, headers, company, clients_to_update
            )

            if response and response.status_code != 200:
                logging.error(response.json())

        if clients_to_update:
            from .crms.serviceTitan_old import update_sold_listed_date_on_location
            for client in clients_to_update:
                client = Client.objects.filter(
                    serv_titan_id=client, company=company).first()
                update_date = ClientUpdate.objects.filter(
                    client=client, status=status).order_by(
                    '-listed').values_list('listed', flat=True)[0]

                if status == "House Recently Sold (6)" and  \
                    company.service_titan_sold_date_custom_field_id or \
                        status == "House For Sale" and \
                        company.service_titan_listed_date_custom_field_id:
                    update_sold_listed_date_on_location.delay(
                        headers, company.id, client.serv_titan_id,
                        status, update_date)

    except Exception as e:
        logging.error("Updating Service Titan clients failed")
        logging.error(f"ERROR: {e}")
        logging.error(traceback.format_exc())

    del_variables(
        [
            headers,
            response,
            payload,
            company,
            status,
            tag_type,
            clients_to_update,
        ]
    )


@shared_task
def add_service_titan_contacted_tag(client, tagId):
    client = Client.objects.get(id=client)
    headers = get_service_titan_access_token(client.company.id)
    payload = {
        "customerIds": [str(client.serv_titan_id)],
        "tagTypeIds": [str(tagId)],
    }
    requests.put(
        url=(
            f"https://api.servicetitan.io/crm/v2/tenant/"
            f"{str(client.company.tenant_id)}/tags"
        ),
        headers=headers,
        json=payload,
        timeout=10,
    )


@shared_task
def remove_all_service_titan_tags(company=None, client=None):
    """
    Remove all Service Titan tags for the provided company or client.

    Parameters:
    company (str, optional): ID of the company.
    client (str, optional): ID of the client.
    """
    if company:
        try:
            company = Company.objects.get(id=company)
            tag_ids = [
                company.service_titan_for_sale_tag_id,
                company.service_titan_recently_sold_tag_id,
            ]
            tag_ids = [str(tag_id) for tag_id in tag_ids if tag_id]

            if tag_ids:
                headers = get_service_titan_access_token(company.id)
                time_limit = datetime.now()

                for tag_id in tag_ids:
                    # get a list of all the serv_titan_ids
                    # for the clients with one from this company
                    clients = list(
                        Client.objects.filter(company=company)
                        .exclude(serv_titan_id=None)
                        .values_list("serv_titan_id", flat=True)
                    )
                    num_iterations = (len(clients) // 250) + 1

                    for i in range(num_iterations):
                        if time_limit < datetime.now() - timedelta(
                            minutes=15
                        ):
                            headers = get_service_titan_access_token(
                                company.id
                            )
                            time_limit = datetime.now()

                        client_subset = clients[
                            i * 250: (i + 1) * 250  # noqa: E203
                        ]
                        payload = {
                            "customerIds": client_subset,
                            "tagTypeIds": [tag_id],
                        }
                        response = handle_tag_deletion_request(
                            payload, headers, company, client_subset
                        )

                        if response and response.status_code != 200:
                            logging.error(response.json())

                Client.objects.filter(company=company).update(
                    status="No Change"
                )

        except Exception as e:
            logging.error("Updating Service Titan clients failed")
            logging.error(f"ERROR: {e}")
            logging.error(traceback.format_exc())

    if client:
        process_client_tags(client)


def update_service_titan_tasks(clients, company, status):
    headers, data, response = "", "", ""
    if clients and (
        company.service_titan_for_sale_tag_id
        or company.service_titan_recently_sold_tag_id
    ):
        try:
            headers = get_service_titan_access_token(company.id)
            response = requests.get(
                url=(
                    f"https://api.servicetitan.io/taskmanagement/"
                    f"v2/tenant/{str(company.tenant_id)}/data"
                ),
                headers=headers,
                timeout=10,
            )
            with open("tasks.json", "w") as f:
                json.dump(response.json(), f)
            # if response.status_code != 200:
            #     resp = response.json()
            #     error = resp["errors"][""][0]
            #     error = (
            #         error.replace("(", "")
            #         .replace(")", "")
            #         .replace(",", " ")
            #         .replace(".", "")
            #         .split()
            #     )
            #     for word in error:
            #         if word.isdigit():
            #             Client.objects.filter(serv_titan_id=word).delete()
            #             forSale.remove(word)
            #     payload = {
            #         "customerIds": forSale,
            #         "taskTypeId": str(company.service_titanTaskID),
            #     }
            #     response = requests.put(
            #         f"https://api.servicetitan.io/crm/v2/tenant/{str(company.tenantID)}/tasks",
            #         headers=headers,
            #         json=payload,
            #     )
        except Exception as e:
            logging.error("updating service titan tasks failed")
            logging.error(f"ERROR: {e}")
            logging.error(traceback.format_exc())
    del_variables([headers, data, response, company, status])


# send email to every customuser with the html
# file that has the same name as the template
def send_update_email(templateName):
    try:
        users = list(
            CustomUser.objects.filter(is_verified=True).values_list(
                "email", flat=True
            )
        )
        mail_subject = "Is My Customer Moving Product Updates"
        messagePlain = """Thank you for signing up for Is My Customer Moving.
          We have some updates for you. Please visit
          https://app.ismycustomermoving.com/ to see them."""
        message = get_template(f"{templateName}.html").render()
        for user in users:
            if "@test.com" not in user.email:
                send_mail(
                    subject=mail_subject,
                    message=messagePlain,
                    from_email=settings.EMAIL_HOST_USER,
                    recipient_list=[user],
                    html_message=message,
                    fail_silently=False,
                )
    except Exception as e:
        logging.error("sending update email failed")
        logging.error(f"ERROR: {e}")
        logging.error(traceback.format_exc())


def filter_home_listings(query_params, queryset, company_id, filter_type):
    """
    Filter all home listings based on the provided query parameters.

    Parameters:
    query_params (dict): Parameters to filter the queryset.
    queryset (QuerySet): QuerySet to be filtered.
    company_id (str): ID of the company.

    Returns:
    queryset: Filtered QuerySet.
    """
    company = Company.objects.get(id=company_id)

    if "saved_filter" in query_params:
        query_params = SavedFilter.objects.get(
            name=query_params["saved_filter"],
            company=company,
            filter_type=filter_type,
        ).saved_filters
        query_params = json.loads(query_params)
        query_params = {k: v for k, v in query_params.items() if v != ""}
        if "tags" in query_params:
            query_params["tags"] = "".join(query_params["tags"])

    for param in query_params:
        if param == "min_price":
            queryset = queryset.filter(price__gte=query_params[param])
        elif param == "max_price":
            queryset = queryset.filter(price__lte=query_params[param])
        elif param == "min_year":
            queryset = queryset.filter(year_built__gte=query_params[param])
        elif param == "max_year":
            queryset = queryset.filter(year_built__lte=query_params[param])
        elif param == "min_beds":
            queryset = queryset.filter(bedrooms__gte=query_params[param])
        elif param == "max_beds":
            queryset = queryset.filter(bedrooms__lte=query_params[param])
        elif param == "min_baths":
            queryset = queryset.filter(bathrooms__gte=query_params[param])
        elif param == "max_baths":
            queryset = queryset.filter(bathrooms__lte=query_params[param])
        elif param == "min_sqft":
            queryset = queryset.filter(sqft__gte=query_params[param])
        elif param == "max_sqft":
            queryset = queryset.filter(sqft__lte=query_params[param])
        elif param == "min_lot_sqft":
            queryset = queryset.filter(lot_sqft__gte=query_params[param])
        elif param == "max_lot_sqft":
            queryset = queryset.filter(lot_sqft__lte=query_params[param])
        elif param in ["min_days_ago", "max_days_ago"]:
            filter_key = (
                "listed__lte" if param == "min_days_ago" else "listed__gte"
            )
            queryset = queryset.filter(
                **{
                    filter_key: (
                        datetime.today()
                        - timedelta(days=int(query_params[param]))
                    ).strftime("%Y-%m-%d")
                }
            )
        elif param == "tags":
            if isinstance(query_params, dict):
                # It's a standard dict, use standard dict access
                tags = query_params.get('tags', [])
                if isinstance(tags, str):
                    # If 'tags' is a single string value, put it in a list
                    tags = [tags]
            else:
                # It's a QueryDict, use the getlist method
                tags = query_params.getlist('tags')
            tags = [tag.replace("_", " ") for tag in tags]
            queryset = queryset.filter(tags__contains=tags)
        elif param in ["state", "city"]:
            filter_key = f"{param}__iexact"
            queryset = queryset.filter(**{filter_key: query_params[param]})
        elif param == "zip_code":
            zip_code = ZipCode.objects.filter(zip_code=query_params[param])
            if zip_code.exists():
                queryset = queryset.filter(zip_code=zip_code.first())
    if "max_days_ago" not in query_params:
        queryset = queryset.filter(
            listed__gte=(datetime.today() - timedelta(days=180)
                         ).strftime("%Y-%m-%d")
        )
    return queryset


def filter_clients(query_params, queryset, company_id):
    """
    Filter clients based on the provided query parameters.

    Parameters:
    query_params (dict): Parameters to filter the queryset.
    queryset (QuerySet): QuerySet to be filtered.

    Returns:
    queryset: Filtered QuerySet.
    """
    company = Company.objects.get(id=company_id)
    if "saved_filter" in query_params:
        query_params = SavedFilter.objects.get(
            name=query_params["saved_filter"],
            company=company,
            filter_type="Client",
        ).saved_filters
        query_params = json.loads(query_params)
        query_params = {k: v for k, v in query_params.items() if v != ""}
        if "tags" in query_params:
            query_params["tags"] = "".join(query_params["tags"])

    for param in query_params:
        if param == "min_price":
            queryset = queryset.filter(price__gte=query_params[param])
        elif param == "max_price":
            queryset = queryset.filter(price__lte=query_params[param])
        elif param == "min_year":
            queryset = queryset.filter(year_built__gte=query_params[param])
        elif param == "max_year":
            queryset = queryset.filter(year_built__lte=query_params[param])
        elif param == "min_beds":
            queryset = queryset.filter(bedrooms__gte=query_params[param])
        elif param == "max_beds":
            queryset = queryset.filter(bedrooms__lte=query_params[param])
        elif param == "min_baths":
            queryset = queryset.filter(bathrooms__gte=query_params[param])
        elif param == "max_baths":
            queryset = queryset.filter(bathrooms__lte=query_params[param])
        elif param == "min_sqft":
            queryset = queryset.filter(sqft__gte=query_params[param])
        elif param == "max_sqft":
            queryset = queryset.filter(sqft__lte=query_params[param])
        elif param == "min_lot_sqft":
            queryset = queryset.filter(lot_sqft__gte=query_params[param])
        elif param == "max_lot_sqft":
            queryset = queryset.filter(lot_sqft__lte=query_params[param])
        elif param == "equip_install_date_min":
            queryset = queryset.filter(
                equipment_installed_date__gte=query_params[param]
            )
        elif param == "equip_install_date_max":
            queryset = queryset.filter(
                equipment_installed_date__lte=query_params[param]
            )
        elif param in ["state", "city"]:
            filter_key = f"{param}__iexact"
            queryset = queryset.filter(**{filter_key: query_params[param]})
        elif param == "zip_code":
            zip_code = ZipCode.objects.filter(zip_code=query_params[param])
            if zip_code.exists():
                queryset = queryset.filter(zip_code=zip_code.first())
        elif param == "tags":
            tags = query_params.getlist('tags')
            tags = [tag.replace("_", " ") for tag in tags]
            queryset = queryset.filter(tags__contains=tags)
        elif param == "client_tags":
            client_tags = query_params.getlist('client_tags')
            queryset = queryset.filter(client_tags__contains=client_tags)
        elif param == "status":
            statuses = []
            if "For Sale" in query_params[param]:
                statuses.append("House For Sale")
            if "Recently Sold" in query_params[param]:
                statuses.append("House Recently Sold (6)")
            if "Off Market" in query_params[param]:
                statuses.append("No Change")
            queryset = queryset.filter(status__in=statuses)
        elif param in ["customer_since_min", "customer_since_max"]:
            filter_key = (
                "service_titan_customer_since__gte"
                if param.endswith("min")
                else "service_titan_customer_since__lte"
            )
            date_value = (
                date(int(query_params[param]), 1, 1)
                if param.endswith("min")
                else date(int(query_params[param]), 12, 31)
            )
            queryset = queryset.filter(**{filter_key: date_value})
        elif param == 'usps_changed':
            queryset = queryset.filter(
                Q(usps_different=True) | Q(usps_address="Error"))
        elif param == "min_revenue":
            queryset = queryset.filter(
                service_titan_lifetime_revenue__gte=query_params[param])
        elif param == "max_revenue":
            queryset = queryset.filter(
                service_titan_lifetime_revenue__lte=query_params[param])

    return queryset


@shared_task
def remove_error_flag():
    old_enough_updates = ClientUpdate.objects.filter(
        error_flag=True, date__lt=datetime.today() - timedelta(days=180)
    )
    for update in old_enough_updates:
        client = update.client
        client.error_flag = False
        client.save()


@shared_task
def verify_address(client_ids):
    """
    Verify the client's address using USPS API.

    Parameters:
    client_id (str): The ID of the client.

    Returns:
    None
    """
    try:
        clients = Client.objects.filter(id__in=client_ids)
    except Exception as e:
        print(e)
        return
    for client in clients:
        zip_code = client.zip_code.zip_code
        base_url = "http://production.shippingapis.com/ShippingAPI.dll"
        user_id = settings.USPS_USER_ID
        api = "Verify"

        xml_request = f"""
        <AddressValidateRequest USERID="{user_id}">
            <Address ID="1">
                <Address1></Address1>
                <Address2>{client.address}</Address2>
                <City>{client.city}</City>
                <State>{client.state}</State>
                <Zip5>{zip_code}</Zip5>
                <Zip4/>
            </Address>
        </AddressValidateRequest>
        """

        params = {"API": api, "XML": xml_request}
        try:
            response = requests.get(base_url, params=params, timeout=10)
        except requests.exceptions.RequestException as e:
            logging.error(e)
            return
        try:
            response_xml = response.text
            parsed_response = fromstring(response_xml)
            address_element = parsed_response.find("Address")
            error = address_element.find("Error")

            if error is None:
                address2 = address_element.find("Address2").text.title()
                address2 = parse_streets(address2)
                city = address_element.find("City").text.title()
                state = address_element.find("State").text
                zip5 = address_element.find("Zip5").text
                if (
                    address2 != client.address
                    or city != client.city
                    or state != client.state
                    or zip5 != zip_code
                ):
                    client.usps_different = True
                client.old_address = (
                    f"{client.address}, {client.city}, "
                    f"{client.state} {client.zip_code.zip_code}"
                )
                client.address = address2
                client.city = city
                client.state = state
                zip, _ = ZipCode.objects.get_or_create(zip_code=zip5)
                client.zip_code = zip
                client.save()
        except Exception as e:
            logging.error(e)
        del_variables([client, zip_code, base_url, user_id, api, xml_request])
    del_variables([client_ids])


@shared_task
def send_zapier_recently_sold(company_id):
    """
    Send information about recently sold homes to Zapier for a given company.

    Parameters:
    company_id (str): The ID of the company.

    Returns:
    None
    """
    try:
        company = Company.objects.get(id=company_id)
    except Company.DoesNotExist:
        logging.error(f"Company with id {company_id} does not exist.")
        return
    if not company.zapier_recently_sold:
        return
    zip_code_objects = Client.objects.filter(company=company).values(
        "zip_code"
    )
    recently_listed_date = (datetime.today() - timedelta(days=7)).strftime(
        "%Y-%m-%d"
    )
    home_listings = HomeListing.objects.filter(
        zip_code__in=zip_code_objects, listed__gt=recently_listed_date,
        status="House Recently Sold (6)"
    ).order_by("listed").select_related('zip_code')

    saved_filters = SavedFilter.objects.filter(
        company=company, filter_type="Recently Sold", for_zapier=True
    )
    for saved_filter in saved_filters:
        filtered_home_listings = filter_home_listings(
            {"saved_filter": saved_filter.name},
            home_listings,
            company_id,
            "Recently Sold"
        )
        if filtered_home_listings:
            try:
                serialized_data = HomeListingSerializer(
                    filtered_home_listings, many=True
                ).data

                for data in serialized_data:
                    # Add saved_filter.name to each item in the list
                    data["filter_name"] = saved_filter.name
                    del data["id"]
                    del data["status"]
                    del data["realtor"]
                    # TODO: Do something with these values
                    # 'roofing': ' ', 'garage_type': ' ',
                    #  'heating': ' ', 'cooling': ' ',
                    # 'heating_cooling_description': ' ',
                    # 'interior_features_description': ' ',
                    # 'exterior': ' ', 'pool': ' ', 'fireplace': ' ',
                    # 'description': ' '

                    requests.post(
                        company.zapier_recently_sold,
                        data=data,
                        timeout=10,
                    )
            except Exception as e:
                logging.error(e)


def format_address_for_scraper(client_id):
    """
    Format the client's address to get details from scraper.

    Parameters:
    client_id (str): The ID of the client.

    Returns:
    str: The formatted address.
    """
    client = Client.objects.get(id=client_id)
    address = f"{client.address}-{client.city}-{client.state}-" \
              f"{client.zip_code.zip_code}"
    address = address.replace(" ", "-")
    return address


@shared_task
def generic_crm_task(crm_class_name, company_id, method_name, **kwargs):
    """
    Generic Celery task to handle various CRM operations.

    Args:
    crm_class_name (str): The name of the CRM class to be instantiated.
    company_id (int): The ID of the company using the CRM.
    method_name (str): The name of the CRM method to call.
    **kwargs: Variable keyword arguments to pass to the CRM method.

    Returns:
    The result of the CRM method call.
    """
    # Construct the module path dynamically
    module_path = f'data.crms.{crm_class_name}'

    # Import the CRM class dynamically
    crm_module = importlib.import_module(module_path)
    crm_class = getattr(crm_module, crm_class_name)

    # Instantiate the CRM class
    crm = crm_class(company_id)

    # Dynamically call the specified method of the CRM instance
    method = getattr(crm, method_name)
    return method(**kwargs)


def chunk_list(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
