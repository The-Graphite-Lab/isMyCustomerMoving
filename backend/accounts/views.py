# Import the required modules at the beginning of the script
from config import settings
import datetime
from django.contrib.auth.hashers import make_password
from django.core.mail import send_mail
from django.http import JsonResponse, Http404, HttpResponse
from django.shortcuts import get_object_or_404, redirect
from django.template.loader import get_template
from django.utils.timezone import make_aware
from functools import wraps
import jwt
import pytz
import pyotp
import requests
import uuid
from rest_framework import generics, status, viewsets
from rest_framework.permissions import AllowAny, IsAuthenticated
from rest_framework.response import Response
from rest_framework.throttling import AnonRateThrottle, UserRateThrottle
from rest_framework.views import APIView
from rest_framework.decorators import api_view
from rest_framework_simplejwt.views import TokenObtainPairView
import logging

from .models import Company, CustomUser, Enterprise, InviteToken
from .serializers import (
    UserSerializer,
    UserSerializerWithToken,
    UserListSerializer,
    MyTokenObtainPairSerializer,
    EnterpriseSerializer,
)
from .utils import create_keap_user
from .constants import INVITE_MESSAGE, INVITE_REMINDER_MESSAGE


def get_token_auth_header(request):
    """
    Obtains the Access Token from the Authorization Header.
    """
    auth = request.META.get("HTTP_AUTHORIZATION", None)
    parts = auth.split()
    token = parts[1]
    return token


def requires_scope(required_scope):
    """
    Determines if the required scope is present in the Access Token.
    Args:
        required_scope (str): The scope required to access the resource.
    """

    def require_scope(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            token = get_token_auth_header(args[0])
            decoded = jwt.decode(token, verify=False)
            if decoded.get("scope"):
                token_scopes = decoded["scope"].split()
                for token_scope in token_scopes:
                    if token_scope == required_scope:
                        return f(*args, **kwargs)
            response = JsonResponse(
                {"message": "You don't have access to this resource"}
            )
            response.status_code = 403
            return response

        return decorated

    return require_scope


class AcceptInvite(APIView):
    permission_classes = [AllowAny]

    def post(self, request, *args, **kwargs):
        invite_token_id = self.kwargs["invitetoken"]
        if not InviteToken.objects.filter(id=invite_token_id).exists():
            return Response(
                {"status": "Invalid Invite Token"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            token = InviteToken.objects.get(
                id=invite_token_id, email=request.data["email"]
            )
            if token.expiration <= pytz.timezone.now():
                return Response(
                    {"status": "Token Expired"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

            company = token.company
            user = get_object_or_404(
                CustomUser,
                email=request.data["email"],
                company=company,
                status="pending",
            )
            user.set_password(request.data["password"])
            user.status = "active"
            user.first_name = request.data["firstName"]
            user.last_name = request.data["lastName"]
            user.phone = request.data["phone"]
            user.is_verified = True
            user.save()
            token.delete()
        except Http404:
            return Response(
                {"status": "Cannot find user"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        serializer = UserSerializerWithToken(user)
        return Response(serializer.data)


class ManageUserView(APIView):
    permission_classes = [IsAuthenticated]

    def post(self, request, *args, **kwargs):
        company_id = self.kwargs.get("id")

        if Company.objects.filter(id=company_id).exists():
            return self._invite_user(request, company_id)
        elif CustomUser.objects.filter(id=company_id).exists():
            return self._make_admin(company_id)
        else:
            return Response(
                {"status": "User Not Found"},
                status=status.HTTP_400_BAD_REQUEST,
            )

    # Invite user helper function
    def _invite_user(self, request, company_id):
        try:
            # Replaced the call to filter and get with a single get call.
            company = Company.objects.get(id=company_id)
            admin = CustomUser.objects.get(company=company, status="admin")

            # Simplified the creation of CustomUser and InviteToken objects.
            user, created_user = CustomUser.objects.get_or_create(
                email=request.data["email"],
                company=company,
                status="pending",
            )
            token, created_token = InviteToken.objects.get_or_create(
                company=company, email=request.data["email"]
            )

            # Simplified the setting of the mail subject and the token expiration.
            mail_subject = (
                INVITE_MESSAGE if created_token else INVITE_REMINDER_MESSAGE
            )
            if not created_token:
                token.expiration = make_aware(
                    datetime.datetime.utcnow() + datetime.timedelta(days=1),
                    timezone=pytz.UTC,
                )
                token.save()

            # Moved the email sending into a helper function.
            self._send_email(
                mail_subject, admin, token, request.data["email"]
            )
            return self._get_response(company, user)

        except Exception as e:
            logging.error(e)
            return Response(
                {"status": "Data Error"}, status=status.HTTP_400_BAD_REQUEST
            )

    # Make admin helper function
    def _make_admin(self, company_id):
        try:
            # Replaced the call to filter and get with a single get call.
            user = CustomUser.objects.get(id=company_id)
            user.status = "admin"
            user.save()

            return self._get_response(user.company, user)

        except Exception as e:
            logging.error(e)
            return Response(
                {"status": "Data Error"}, status=status.HTTP_400_BAD_REQUEST
            )

    # Send email helper function
    def _send_email(self, mail_subject, admin, token, recipient):
        message_plain = INVITE_MESSAGE.format(
            admin.first_name, admin.last_name, str(token.id)
        )
        message_html = get_template("addUserEmail.html").render(
            {"admin": admin, "token": token.id}
        )
        send_mail(
            mail_subject,
            message_plain,
            "your-email@example.com",  # Replace with your actual email
            [recipient],
            fail_silently=False,
            html_message=message_html,
        )


def put(self, request, *args, **kwargs):
    try:
        if CustomUser.objects.filter(id=self.kwargs["id"]).exists():
            user = CustomUser.objects.get(id=self.kwargs["id"])
            if user:
                # Ensure provided email is not already in use by another user
                if (
                    CustomUser.objects.filter(email=request.data["email"])
                    .exclude(id=user.id)
                    .exists()
                ):
                    return Response(
                        {"status": "Email already in use"},
                        status=status.HTTP_400_BAD_REQUEST,
                    )

                user.first_name = request.data.get(
                    "firstName", user.first_name
                )
                user.last_name = request.data.get("lastName", user.last_name)
                user.email = request.data.get("email", user.email)
                if request.data.get("phone"):
                    user.phone = request.data["phone"]
                user.save()
                create_keap_user(user.id)
            serializer = UserSerializerWithToken(user, many=False)
            return Response(serializer.data)
        else:
            return Response(
                {"status": "User Not Found"},
                status=status.HTTP_400_BAD_REQUEST,
            )
    except Exception as e:
        logging.error(e)
        return Response(
            {"status": "Data Error"}, status=status.HTTP_400_BAD_REQUEST
        )


class RegisterView(APIView):
    permission_classes = [AllowAny]
    throttle_classes = [AnonRateThrottle]
    authentication_classes = []

    def post(self, request):
        data = request.data
        first_name = data.get("firstName")
        last_name = data.get("lastName")
        email = data.get("email")
        password = data.get("password")
        company = data.get("company")
        registrationToken = data.get("registrationToken")
        phone = data.get("phone")
        messages = {"errors": []}
        if first_name is None:
            messages["errors"].append("first_name can't be empty")
        if last_name is None:
            messages["errors"].append("last_name can't be empty")
        if email is None:
            messages["errors"].append("Email can't be empty")
        if password is None:
            messages["errors"].append("Password can't be empty")
        if company is None:
            messages["errors"].append("Company can't be empty")
        if registrationToken is None:
            messages["errors"].append("Registration Token can't be empty")

        if CustomUser.objects.filter(email=email).exists():
            messages["errors"].append(
                "Account already exists with this email id."
            )
        if len(messages["errors"]) > 0:
            return Response(
                {"detail": messages["errors"]},
                status=status.HTTP_400_BAD_REQUEST,
            )
        try:
            company = Company.objects.get(
                name=company, accessToken=registrationToken
            )
            noAdmin = CustomUser.objects.filter(
                company=company, isVerified=True
            )
            if len(noAdmin) == 0:
                user = CustomUser.objects.create(
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    password=make_password(password, salt=uuid.uuid4().hex),
                    company=company,
                    status="admin",
                    isVerified=True,
                    phone=phone,
                )
                # user = CustomUser.objects.get(email=email)
                # current_site = get_current_site(request)
                # tokenSerializer = UserSerializerWithToken(user, many=False)
                # mail_subject = "Activation Link for Is My Customer Moving"
                # messagePlain = """Verify your account for Is My Customer Moving
                # by going here {}/api/v1/accounts/confirmation/{}/{}/"""
                # .format(current_site, tokenSerializer.data['refresh'], user.id)
                # message = get_template("registration.html").render({
                #     'current_site': current_site,
                #       'token': tokenSerializer.data['refresh'],
                #       'user_id': user.id
                # })
                # send_mail(
                # subject=mail_subject,
                # message=messagePlain,
                # from_email=settings.EMAIL_HOST_USER,
                #  recipient_list=[email],
                #  html_message=message,
                #  fail_silently=False
                # )
                serializer = UserSerializerWithToken(user, many=False)
                create_keap_user(user.id)
            else:
                return Response(
                    {
                        "detail": """Access Token Already Used.
                        Ask an admin to login and create profile for you."""
                    },
                    status=status.HTTP_400_BAD_REQUEST,
                )
        except Exception as e:
            logging.error(e)
            return Response(
                {"detail": f"{e}"}, status=status.HTTP_400_BAD_REQUEST
            )
        return Response(serializer.data)


@api_view(["GET"])
def confirmation(request, pk, uid):
    user = CustomUser.objects.get(id=uid)
    token = jwt.decode(pk, settings.SECRET_KEY, algorithms=["HS256"])

    if (
        not user.isVerified
        and datetime.datetime.fromtimestamp(token["exp"])
        > datetime.datetime.now()
    ):
        user.isVerified = True
        user.save()
        return redirect("http://www.ismycustomermoving.com/login")

    elif (
        datetime.datetime.fromtimestamp(token["exp"])
        < datetime.datetime.now()
    ):
        # For resending confirmation email
        #  use send_mail with the following encryption
        # print(jwt.encode({
        # 'user_id': user.user.id,
        #  'exp': datetime.datetime.now() + datetime.timedelta(days=1)},
        #  settings.SECRET_KEY, algorithm='HS256'))

        return HttpResponse("Your activation link has been expired")
    else:
        return redirect("http://www.ismycustomermoving.com/login")


class MyTokenObtainPairView(TokenObtainPairView):
    serializer_class = MyTokenObtainPairSerializer

    def post(self, request, *args, **kwargs):
        # Call the serializer class to validate the data
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        # Return the validated data
        return Response(serializer.validated_data, status=status.HTTP_200_OK)


class UserViewSet(viewsets.ReadOnlyModelViewSet):
    throttle_classes = [UserRateThrottle]
    serializer_class = UserSerializer
    queryset = CustomUser.objects.all()
    permission_classes = [IsAuthenticated]


class OTPGenerateView(generics.GenericAPIView):
    permission_classes = [IsAuthenticated]
    throttle_classes = [AnonRateThrottle]
    authentication_classes = []

    def post(self, request):
        try:
            user = CustomUser.objects.get(id=request.data["id"])
            otp_base32 = pyotp.random_base32()
            otp_auth_url = pyotp.totp.TOTP(otp_base32).provisioning_uri(
                name=user.email.lower(), issuer_name="Is My Customer Moving"
            )

            user.otp_auth_url = otp_auth_url
            user.otp_base32 = otp_base32
            user.save()
            serializer = UserSerializerWithToken(user)
            return Response(serializer.data, status=status.HTTP_200_OK)

        except CustomUser.DoesNotExist:
            return Response(
                {"detail": "User with this ID does not exist"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            logging.error(e)
            return Response(
                {"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST
            )


class OTPVerifyView(generics.GenericAPIView):
    permission_classes = [IsAuthenticated]
    throttle_classes = [AnonRateThrottle]
    authentication_classes = []

    def post(self, request):
        try:
            user = CustomUser.objects.get(id=request.data["id"])
            if not user.otp_base32:
                return Response(
                    {"detail": "OTP not generated for this user"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            totp = pyotp.TOTP(user.otp_base32)
            if totp.verify(request.data["otp"]):
                user.otp_enabled = True
                user.otp_verified = True
                user.save()
                serializer = UserSerializerWithToken(user)
                return Response(serializer.data, status=status.HTTP_200_OK)
            else:
                logging.error("verification failed")
                return Response(
                    {"detail": "OTP verification failed"},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        except CustomUser.DoesNotExist:
            return Response(
                {"detail": "User with this ID does not exist"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            logging.error(e)
            return Response(
                {"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST
            )


class OTPValidateView(generics.GenericAPIView):
    permission_classes = [IsAuthenticated]
    throttle_classes = [AnonRateThrottle]
    authentication_classes = []

    def post(self, request):
        otp = request.data["otp"]
        messages = {"errors": []}
        try:
            user = CustomUser.objects.get(id=request.data["id"])
            if not user.otp_verified:
                messages["errors"].append("One Time Password incorrect")
            if not user.otp_base32:
                return Response(
                    {"detail": "OTP not generated for this user"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            if messages["errors"]:
                return Response(
                    {"detail": messages["errors"]},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            totp = pyotp.TOTP(user.otp_base32)
            if totp.verify(otp, valid_window=1):
                serializer = UserSerializerWithToken(user)
                return Response(serializer.data, status=status.HTTP_200_OK)
            else:
                return Response(
                    {"detail": "OTP verification failed"},
                    status=status.HTTP_400_BAD_REQUEST,
                )
        except CustomUser.DoesNotExist:
            return Response(
                {"detail": "User with this ID does not exist"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            logging.error(e)
            return Response(
                {"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST
            )


class OTPDisableView(generics.GenericAPIView):
    permission_classes = [IsAuthenticated]
    throttle_classes = [AnonRateThrottle]
    authentication_classes = []

    def post(self, request):
        try:
            user = CustomUser.objects.get(id=request.data["id"])
            user.otp_enabled = False
            user.otp_verified = False
            user.otp_base32 = None
            user.otp_auth_url = None
            user.save()
            serializer = UserSerializerWithToken(user)
            return Response(serializer.data, status=status.HTTP_200_OK)

        except CustomUser.DoesNotExist:
            return Response(
                {"detail": "User with this ID does not exist"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            logging.error(e)
            return Response(
                {"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST
            )


class UserListView(generics.ListAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = UserListSerializer

    def get_queryset(self):
        return CustomUser.objects.filter(company=self.kwargs["company_id"])


# PEP8 compliance recommends using snake_case for variable names
class TokenValidateView(APIView):
    def get(self, request, *args, **kwargs):
        client_id = settings.GOOGLE_CLIENT_ID
        access_token = request.headers.get("Authorization")
        if access_token is None:
            return Response(
                {"detail": "Access token missing"},
                status=status.HTTP_403_FORBIDDEN,
            )

        access_token = access_token.split(" ")[1]
        # Removed unnecessary triple quotes around string
        base_url = "https://www.googleapis.com/oauth2/v3"
        token_info_url = f"{base_url}/tokeninfo?access_token={access_token}"
        token_info = requests.get(
            token_info_url,
            timeout=10,
        ).json()

        if int(token_info["expires_in"]) <= 0:
            return Response(
                {"detail": "Access token expired"},
                status=status.HTTP_401_UNAUTHORIZED,
            )

        if token_info["aud"] != client_id:
            return Response(
                {"detail": "Invalid access token"},
                status=status.HTTP_401_UNAUTHORIZED,
            )
        user_info_url = f"{base_url}/userinfo?access_token={access_token}"
        user_info = requests.get(
            user_info_url,
            timeout=10,
        ).json()

        if token_info["sub"] != user_info["sub"]:
            return Response(
                {"detail": "Token and user information mismatch"},
                status=status.HTTP_401_UNAUTHORIZED,
            )

        user_data = {
            "email": user_info["email"],
            "username": user_info["name"],
            "password": "randompassword",
        }
        try:
            CustomUser.objects.get(email=user_data["email"])
            response = requests.post(
                f"{settings.BASE_BACKEND_URL}/api/v1/accounts/glogin/",
                json=user_data,
                timeout=10,
            ).json()
            return Response(response, status=status.HTTP_200_OK)
        except CustomUser.DoesNotExist:
            return Response(
                {"detail": "User with provided email does not exist"},
                status=status.HTTP_400_BAD_REQUEST,
            )


class GoogleLoginViewSet(viewsets.ViewSet):
    def create(self, request):
        email = request.data["email"]
        try:
            user = CustomUser.objects.get(email=email)
        except CustomUser.DoesNotExist:
            return Response(
                {"detail": "User with this email does not exist"},
                status=status.HTTP_404_NOT_FOUND,
            )

        serializer = UserSerializerWithToken(user)
        return Response(serializer.data, status=status.HTTP_302_FOUND)


class ZapierTokenView(TokenObtainPairView):
    serializer_class = MyTokenObtainPairSerializer

    def post(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        return Response(
            {"access": serializer.validated_data["access"]},
            status=status.HTTP_200_OK,
        )


class AuthenticatedUserView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        try:
            user = CustomUser.objects.get(email=request.user)
            serializer = UserSerializer(user)
            return Response(serializer.data["first_name"])
        except CustomUser.DoesNotExist:
            return Response(
                {"detail": "User with this email does not exist"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        except Exception as e:
            logging.error(e)
            return Response(
                {"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST
            )


class ZapierSoldSubscribeView(APIView):
    """
    Handles Zapier Sold subscriptions.
    """

    permission_classes = [IsAuthenticated]

    def get(self, request):
        """
        Returns test data for a client.

        :param request: The HTTP request
        :return: The HTTP response containing test client data
        """
        try:
            test_client = [
                {
                    "name": "new Test Data",
                    "address": "123 Main St",
                    "city": "New York",
                    "state": "NY",
                    "zipCode": 10001,
                    "phoneNumber": "212-555-1234",
                }
            ]
            return Response(test_client, status=status.HTTP_200_OK)
        except Exception as e:
            logging.error(e)
            return Response(
                {"detail": f"{e}"}, status=status.HTTP_400_BAD_REQUEST
            )

    def post(self, request):
        """
        Updates the Zapier Sold hook URL for the company associated with the user.

        :param request: The HTTP request
        :return: The HTTP response
        """
        try:
            user = CustomUser.objects.get(email=request.user)
            company = user.company
            company.zapier_sold = request.data["hookUrl"]
            company.save()
            return Response(
                {"detail": "Zapier Sold Subscribe"}, status=status.HTTP_200_OK
            )
        except Exception as e:
            logging.error(e)
            return Response(
                {"detail": f"{e}"}, status=status.HTTP_400_BAD_REQUEST
            )

    def delete(self, request):
        """
        Removes the Zapier Sold hook URL for the company associated with the user.

        :param request: The HTTP request
        :return: The HTTP response
        """
        try:
            user = CustomUser.objects.get(email=request.user)
            company = user.company
            company.zapier_sold = None
            company.save()
            return Response(
                {"detail": "Zapier Sold Unsubscribe"},
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            logging.error(e)
            return Response(
                {"detail": f"{e}"}, status=status.HTTP_400_BAD_REQUEST
            )


class ZapierForSaleSubscribeView(APIView):
    """
    Handles Zapier For Sale subscriptions.
    """

    permission_classes = [IsAuthenticated]

    def get(self, request):
        """
        Returns test data for a client.

        :param request: The HTTP request
        :return: The HTTP response containing test client data
        """
        try:
            test_client = [
                {
                    "name": "really new Test Data",
                    "address": "123 Main St",
                    "city": "New York",
                    "state": "NY",
                    "zipCode": 10001,
                    "phoneNumber": "212-555-1234",
                }
            ]
            return Response(test_client, status=status.HTTP_200_OK)
        except Exception as e:
            logging.error(e)
            return Response(
                {"detail": f"{e}"}, status=status.HTTP_400_BAD_REQUEST
            )

    def post(self, request):
        """
        Updates the Zapier For Sale hook URL
        for the company associated with the user.

        :param request: The HTTP request
        :return: The HTTP response
        """
        try:
            user = CustomUser.objects.get(email=request.user)
            company = user.company
            company.zapier_forSale = request.data["hookUrl"]
            company.save()
            return Response(
                {"detail": "Zapier For Sale Subscribe"},
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            logging.error(e)
            return Response(
                {"detail": f"{e}"}, status=status.HTTP_400_BAD_REQUEST
            )

    def delete(self, request):
        """
        Removes the Zapier For Sale hook URL
        for the company associated with the user.

        :param request: The HTTP request
        :return: The HTTP response
        """
        try:
            user = CustomUser.objects.get(email=request.user)
            company = user.company
            company.zapier_forSale = None
            company.save()
            return Response(
                {"detail": "Zapier For Sale Unsubscribe"},
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            logging.error(e)
            return Response(
                {"detail": f"{e}"}, status=status.HTTP_400_BAD_REQUEST
            )


class ZapierRecentlySoldSubscribeView(APIView):
    """
    Handles Zapier Recently Sold subscriptions.
    """

    permission_classes = [IsAuthenticated]

    def get(self, request):
        """
        Returns test data for a recently sold home listing.

        :param request: The HTTP request
        :return: The HTTP response containing test home listing data
        """
        try:
            test_recently_sold_home_listing = [
                {
                    "address": "123 Main St",
                    "city": "New York",
                    "state": "NY",
                    "zipCode": 10001,
                    "price": 1000000,
                    "housingType": "Single Family",
                    "bedrooms": 3,
                    "bathrooms": 2,
                    "yearBuilt": 1990,
                    "sqft": 2000,
                    "lot_sqft": 5000,
                    "roofing": "Shingle",
                    "garage_type": "Attached",
                    "garage": 2,
                    "heating": "Forced Air",
                    "cooling": "Central",
                    "exterior": "Brick",
                    "pool": "Inground",
                    "fireplace": "Yes",
                    "tags": ["New", "Pool", "Fireplace"],
                    "filterName": "Test Filter",
                }
            ]
            return Response(
                test_recently_sold_home_listing, status=status.HTTP_200_OK
            )
        except Exception as e:
            logging.error(e)
            return Response(
                {"detail": f"{e}"}, status=status.HTTP_400_BAD_REQUEST
            )

    def post(self, request):
        """
        Updates the Zapier Recently Sold hook URL
        for the company associated with the user.

        :param request: The HTTP request
        :return: The HTTP response
        """
        try:
            user = CustomUser.objects.get(email=request.user)
            company = user.company
            company.zapier_recentlySold = request.data["hookUrl"]
            company.save()
            return Response(
                {"detail": "Zapier Recently Sold Subscribe"},
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            logging.error(e)
            return Response(
                {"detail": f"{e}"}, status=status.HTTP_400_BAD_REQUEST
            )

    def delete(self, request):
        """
        Removes the Zapier Recently Sold hook URL
        for the company associated with the user.

        :param request: The HTTP request
        :return: The HTTP response
        """
        try:
            user = CustomUser.objects.get(email=request.user)
            company = user.company
            company.zapier_recentlySold = None
            company.save()
            return Response(
                {"detail": "Zapier Recently Sold Unsubscribe"},
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            logging.error(e)
            return Response(
                {"detail": f"{e}"}, status=status.HTTP_400_BAD_REQUEST
            )


class UserEnterpriseView(APIView):
    permission_classes = [IsAuthenticated]

    def get_object(self, enterprise_id):
        try:
            return Enterprise.objects.get(id=enterprise_id)
        except Enterprise.DoesNotExist:
            raise Http404

    def get(self, request, *args, **kwargs):
        if not request.user.enterprise:
            return Response(
                {"detail": "User not part of any enterprise"},
                status=status.HTTP_400_BAD_REQUEST,
            )

        enterprise = self.get_object(request.user.enterprise.id)
        serializer = EnterpriseSerializer(enterprise)
        return Response(serializer.data)

    def put(self, request, *args, **kwargs):
        if not request.user.is_enterprise_owner:
            return Response(
                {"detail": "User is not an enterprise owner"},
                status=status.HTTP_403_FORBIDDEN,
            )

        company_id = request.data.get("company_id")
        try:
            company = Company.objects.get(id=company_id)
        except Company.DoesNotExist:
            return Response(
                {"detail": "Company not found"},
                status=status.HTTP_404_NOT_FOUND,
            )

        request.user.company = company
        request.user.save()
        serializer = UserSerializerWithToken(request.user)
        return Response(serializer.data)
