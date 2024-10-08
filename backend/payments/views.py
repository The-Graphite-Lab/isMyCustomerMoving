from django.conf import settings
# from django.template.loader import get_template
# from django.core.mail import send_mail
from rest_framework import status
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.views import APIView


from accounts.models import Company, CustomUser
from accounts.utils import make_company, create_keap_company
from data.utils import delete_extra_clients
from .models import Product
from .serializers import ProductSerializer

# from datetime import datetime, timedelta

import stripe
import logging

stripe.api_key = settings.STRIPE_LIVE_SECRET_KEY

logger = logging.getLogger(__name__)


class StripeWebhook(APIView):
    permission_classes = [AllowAny]
    serializer_class = ProductSerializer

    def post(self, request, *args, **kwargs):
        event_type = request.data.get("type")
        if not event_type:
            logger.error("Received a Stripe webhook without type")
            return Response(status=status.HTTP_400_BAD_REQUEST)

        print(f"Received a Stripe webhook of type {event_type}")
        if event_type == "checkout.session.completed":
            try:
                logging.info("checkout session completed")
                obj = request.data["data"]["object"]
                phone = obj["customer_details"]["phone"]
                email = obj["customer_details"]["email"]
                company_name = obj["custom_fields"][0]["text"]["value"]
                stripe_id = obj["customer"]
                subscription = stripe.Subscription.retrieve(
                    obj["subscription"])

                logging.info(f"company name: {company_name}")
                product = Product.objects.get(id=subscription["plan"]["id"])

                try:
                    company = Company.objects.get(
                        name=company_name, email=email, stripe_id=stripe_id
                    )
                    logging.info(f"company in try retrieved: {company}")
                    company.stripe_id = stripe_id
                    company.product = product
                    company.phone = phone
                    company.save()
                    logging.info(f"company in try updated: {company}")
                    create_keap_company(company.id)
                except Company.DoesNotExist:
                    try:
                        company = make_company(
                            company_name, email, phone, stripe_id
                        )
                        logging.info(f"company in except: {company}")
                        company = Company.objects.get(id=company)
                        company.product = product
                        company.save()
                        logging.info(f"company in except created: {company}")
                        create_keap_company(company.id)
                    except Exception as e:
                        logging.error(f"error: {e}")
                        return Response(status=status.HTTP_400_BAD_REQUEST)

                users = CustomUser.objects.filter(company=company)
                for user in users:
                    user.is_verified = True
                    user.save()
                delete_extra_clients(company.id)
                return Response(status=status.HTTP_200_OK)
            except Exception as e:
                logging.error(f"error: {e}")
                return Response(status=status.HTTP_400_BAD_REQUEST)

        elif event_type == "price.created":
            try:
                price = request.data["data"]["object"]["unit_amount_decimal"]
                Product.objects.create(
                    id=request.data["data"]["object"]["id"],
                    amount=(
                        float(price)/100),
                    interval=request.data["data"]["object"]["recurring"]["interval"]
                )
                return Response(status=status.HTTP_200_OK)
            except Exception as e:
                logging.error(f"error: {e}")
                return Response(status=status.HTTP_400_BAD_REQUEST)

        elif event_type == "customer.subscription.created":
            print("subscription created")
            return Response(status=status.HTTP_200_OK)

        #     # try:
        #     #     obj = request.data["data"]["object"]
        #     #     customer = Company.objects.get(stripe_id=obj["customer"])

        #     #     product = Product.objects.get(
        #     #         pid=obj["items"]["data"][0]["price"]["id"]
        #     #     )
        #     #     company.product = product
        #     #     company.save()
        #     #     users = CustomUser.objects.filter(company=company)
        #     #     for user in users:
        #     #         user.isVerified = True
        #     #         user.save()
        #     #     return Response(status=status.HTTP_200_OK)
        #     # except Exception as e:
        #     #     print(e)
        #     #     print("error")
        #     #     return Response(status=status.HTTP_400_BAD_REQUEST)

        elif event_type == "customer.subscription.updated":
            print("subscription updated")
            return Response(status=status.HTTP_200_OK)
        #     # try:
        #     #     obj = request.data["data"]["object"]
        #     #     customer_data = stripe.Customer.list(email=email).data
        #     #     customer = customer_data[0]
        #     #     product = Product.objects.get(id=customer.plan.id)

        #     #     company.product = product
        #     #     company.save()
        #     #     if product.id != "price_1MhxfPAkLES5P4qQbu8O45xy":
        #     #         delete_extra_clients(company.id)
        #     # except Exception as e:
        #     #     print(e)
        #     #     print("error")
        # # canceled subscription
        elif event_type == "customer.subscription.deleted":
            print("subscription deleted")
            try:
                obj = request.data["data"]["object"]
                company = Company.objects.get(stripe_id=obj["customer"])
                product = Product.objects.get(
                    id="price_1MhxfPAkLES5P4qQbu8O45xy"
                )
                company.product = product
                company.save()
                users = CustomUser.objects.filter(company=company)
                for user in users:
                    user.isVerified = False
                    user.save()
                    # mail_subject = "Subscription Ended: Is My Customer Moving"
                    # # send the endedSubscrition email to each user
                    # messagePlain = """Your subscription to Is My Customer Moving
                    #     has ended. Please contact us at reid@ismycustomermoving.com
                    #     to reactivate your subscription."""
                    # message = get_template("endedSubscription.html").render(
                    #     {"email": user.email}
                    # )
                    # send_mail(
                    #     subject=mail_subject,
                    #     message=messagePlain,
                    #     from_email=settings.EMAIL_HOST_USER,
                    #     recipient_list=[user.email],
                    #     html_message=message,
                    #     fail_silently=False,
                    # )
            except Exception as e:
                print(e)
                print("error")
            return Response(status=status.HTTP_200_OK)
        elif event_type == "customer.created":
            print("customer created")
            return Response(status=status.HTTP_200_OK)
        else:
            print("event type not found")
            return Response(status=status.HTTP_200_OK)
