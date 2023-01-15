from rest_framework import serializers
from .models import CustomUser, Company, Client, ClientUpdate
from payments.models import Product
from rest_framework_simplejwt.tokens import RefreshToken
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from django.utils.crypto import get_random_string
from django.contrib.auth.models import update_last_login


class CompanySerializer(serializers.ModelSerializer):
    id = serializers.UUIDField(read_only=True)
    name = serializers.CharField(max_length=100)
    phone = serializers.CharField(max_length=20)
    email = serializers.EmailField(max_length=100)
    stripeID = serializers.CharField(max_length=100, required=False)
    tenantID = serializers.CharField(max_length=100, required=False)
    clientID = serializers.CharField(max_length=100, required=False)
    serviceTitanForRentTagID = serializers.CharField(max_length=100, required=False)
    serviceTitanForSaleTagID = serializers.CharField(max_length=100, required=False)
    serviceTitanRecentlySoldTagID = serializers.CharField(max_length=100, required=False)

    def create(self, validated_data):
        if Company.objects.filter(name=validated_data['name']).exists():
            return False
        return Company.objects.create(**validated_data, accessToken=get_random_string(length=32))
    class Meta:
        model = Company
        fields=['id', 'name', 'phone', 'email', 'tenantID', 'clientID', 'stripeID', 'serviceTitanForRentTagID', 'serviceTitanForSaleTagID', 'serviceTitanRecentlySoldTagID']

class MyTokenObtainPairSerializer(TokenObtainPairSerializer):
    def validate(self, attrs):
        data = super().validate(attrs)
        serializer = UserSerializerWithToken(self.user).data
        update_last_login(None, self.user)
        for k, v in serializer.items():
            data[k] = v
        return data
    @classmethod
    def get_token(cls, user):
        token = super().get_token(user)
        if user.isVerified:
            return token
        else:
            raise serializers.ValidationError("User is not verified")

class UserSerializer(serializers.Serializer):
    id = serializers.UUIDField(read_only=True)
    avatarUrl = serializers.ImageField(read_only=True)
    first_name = serializers.CharField(read_only=True)
    last_name = serializers.CharField(read_only=True)
    
    email = serializers.EmailField(max_length=100)
    isVerified = serializers.BooleanField(read_only=True)
    status = serializers.CharField(read_only=True)
    role = serializers.CharField(read_only=True)
    company = CompanySerializer(read_only=True)
    accessToken = serializers.CharField(read_only=True)
    finishedSTIntegration = serializers.SerializerMethodField(read_only=True)

    def get_finishedSTIntegration(self, obj):
        return obj.company.tenantID != None and obj.company.clientID != None and obj.company.clientSecret != None


class UserSerializerWithToken(UserSerializer):
    access = serializers.SerializerMethodField(read_only=True)
    refresh = serializers.SerializerMethodField(read_only=True)

    def get_access(self, obj):
        token = RefreshToken.for_user(obj)
        return str(token.access_token)

    def get_refresh(self, obj):
        token = RefreshToken.for_user(obj)
        return str(token)

class UploadFileSerializer(serializers.Serializer):
    file = serializers.FileField()
    # company = serializers.CharField()

class ClientUpdateSerializer(serializers.ModelSerializer):
    status = serializers.CharField(max_length=100)
    date = serializers.DateField()
    listed = serializers.CharField(max_length=100)
    note = serializers.CharField(max_length=100)
    contacted = serializers.BooleanField()

    class Meta:
        model = ClientUpdate
        fields = ('id', 'status', 'date', 'listed', 'note', 'contacted')
        read_only_fields = fields

class ClientListSerializer(serializers.ModelSerializer):
    id = serializers.UUIDField(read_only=True)
    name = serializers.CharField(max_length=100)
    address = serializers.CharField(max_length=100)
    city = serializers.CharField(max_length=100)
    state = serializers.CharField(max_length=100)
    zipCode = serializers.CharField(max_length=100)
    status = serializers.CharField(max_length=100)
    contacted = serializers.BooleanField()
    note = serializers.CharField(max_length=100)
    clientUpdates = ClientUpdateSerializer(many=True, read_only=True)

    class Meta:
        model = Client
        fields = ('id', 'name', 'address', 'city', 'state', 'zipCode', 'status', 'contacted', 'note', 'clientUpdates')
        read_only_fields = fields

class UserListSerializer(serializers.ModelSerializer):
    class Meta:
        model = CustomUser
        fields = ('id', 'first_name', 'last_name', 'email', 'status')

