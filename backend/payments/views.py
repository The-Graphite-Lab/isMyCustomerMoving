from rest_framework.response import Response
from rest_framework import status
from rest_framework.decorators import api_view
from django.conf import settings
from .models import Product
from accounts.utils import makeCompany
from datetime import datetime, timedelta

from django.utils import timezone
from djstripe import webhooks, models as djstripe_models



logger = logging.getLogger(__name__)

import stripe

stripe.api_key = settings.STRIPE_LIVE_SECRET_KEY

@api_view(['POST' ])
def save_stripe_info(request):
    if request.method == 'POST':
        data = request.data
        email = data['email']
        company = data['company']
        phone = data['phone']
        payment_method_id = data['payment_method_id']
        try:
            product = Product.objects.get(tier=data['tier'], timeFrame=data['timeFrame'])
        except Product.DoesNotExist:
            print("Product does not exist")
            return Response("Product does not exist", status=status.HTTP_400_BAD_REQUEST)
        extra_msg = ''
        customer_data = stripe.Customer.list(email=email).data
        if len(customer_data) == 0:
            customer = stripe.Customer.create(
            email=email, payment_method=payment_method_id, name=company, invoice_settings={
                'default_payment_method': payment_method_id
            })
        else:
            customer = customer_data[0]
            extra_msg = 'Customer already exists.'
        
        trialEnd = int((datetime.now()+ timedelta(days=7)).timestamp())
        subscription = stripe.Subscription.create(
            customer=customer,
            items=[
                {
                'price': product.pid
                }
            ],
            trial_end=trialEnd
        )
        comp = makeCompany(company, email, phone, customer.id)
        comp.product = product
        comp.save()
        if type(comp) == dict:
            print("company equals dict")
            return Response(comp, status=status.HTTP_400_BAD_REQUEST)
        return Response(status=status.HTTP_200_OK, 
            data={
                'message': 'Success', 
                'data': {'customer_id': customer.id,
                'extra_msg': extra_msg,
                'sub_id':  subscription,
                } }  
        )

@api_view(['POST'])
def setup_intent(request):
    if request.method == 'POST':
        intent = stripe.SetupIntent.create(
            payment_method_types=['card'],
        )
        return Response(status=status.HTTP_200_OK,
            data={
                'message': 'Success',
                'data': {'client_secret': intent.client_secret } }
        )
        
@api_view(['GET'])
def publishable_key(request):
    if request.method == 'GET':
        return Response(status=status.HTTP_200_OK,
            data={
                'message': 'Success',
                'data': {'publishable_key': settings.STRIPE_PUBLISHABLE_KEY } }
        )


@webhooks.handler('customer.created')
def create_customer(event: djstripe_models.Event):
    print("wassup duddddde")
    obj = event.data['object']
    print(obj)
    print("done")

# @webhooks.handler('subscription_schedule.released')
# def capture_release_schedule(event: djstripe_models.Event):
#     """
#     Since we mostly operate on subscription schedules, in case the subscription is released by whatever reason
#     we want to assign it to a schedule again

#     :param event:
#     :return:
#     """
#     obj = event.data['object']
#     subscriptions.create_schedule(subscription=obj['released_subscription'])


# @webhooks.handler('payment_method.attached')
# def update_subscription_default_payment_method(event: djstripe_models.Event):
#     """
#     Remove this webhook if you don't want the newest payment method
#     to be a default one for the subscription.
#     The best alternative approach would most likely be to create a custom API
#     endpoint that sets a default payment method on demand called right after
#     the web app succeeds setup intent confirmation.

#     :param event:
#     :return:
#     """

#     obj = event.data['object']
#     customer = event.customer
#     if customer.default_payment_method is None:
#         customers.set_default_payment_method(customer=customer, payment_method=obj['id'])


# @webhooks.handler('payment_method.detached')
# def remove_detached_payment_method(event: djstripe_models.Event):
#     obj = event.data['object']
#     djstripe_models.PaymentMethod.objects.filter(id=obj['id']).delete()


# @webhooks.handler('invoice.payment_failed', 'invoice.payment_action_required')
# def cancel_trial_subscription_on_payment_failure(event: djstripe_models.Event):
#     obj = event.data['object']
#     subscription_id = obj.get('subscription', None)

#     subscription: djstripe_models.Subscription = djstripe_models.Subscription.objects.get(id=subscription_id)

#     # Check if the previous subscription period was trialing
#     # Unfortunately status field is already updated to active at this point
#     if subscription.current_period_start == subscription.trial_end:
#         subscription.cancel(at_period_end=False)


# @webhooks.handler('invoice.payment_failed', 'invoice.payment_action_required')
# def send_email_on_subscription_payment_failure(event: djstripe_models.Event):
#     """
#     This is an example of a handler that sends an email to a customer after a recurring payment fails

#     :param event:
#     :return:
#     """
#     notifications.SubscriptionErrorEmail(customer=event.customer).send()


# @webhooks.handler('customer.subscription.trial_will_end')
# def send_email_trial_expires_soon(event: djstripe_models.Event):
#     obj = event.data['object']
#     expiry_date = timezone.datetime.fromtimestamp(obj['trial_end'], tz=timezone.timezone.utc)
#     notifications.TrialExpiresSoonEmail(customer=event.customer, data={'expiry_date': expiry_date}).send()
