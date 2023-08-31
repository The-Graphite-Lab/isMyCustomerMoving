from rest_framework import serializers
import drf_serpy as serpy


from accounts.serializers import EnterpriseSerializer, BasicCompanySerializer
from .models import (
    Client,
    ClientUpdate,
    HomeListing,
    HomeListingTags,
    Realtor,
    Referral
)


# class ClientUpdateSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = ClientUpdate
#         fields = [f.name for f in ClientUpdate._meta.fields]
#         read_only_fields = fields

class ClientUpdateSerializer(serpy.Serializer):
    id = serpy.Field()
    client = serpy.Field(attr='client.id')
    date = serpy.Field()
    status = serpy.Field()
    listed = serpy.Field()
    note = serpy.Field()
    contacted = serpy.Field()
    error_flag = serpy.Field()


class ClientSerializer(serializers.ModelSerializer):
    class Meta:
        model = Client
        fields = [
            "name",
            "address",
            "city",
            "state",
            "zip_code",
        ]


# class ClientListSerializer(serpy.Serializer):
#     zip_code = serpy.Field(source='zip_code.zip_code')
#     zip_code = serializers.CharField(source='zip_code.zip_code')
#     tag = serializers.SerializerMethodField()
#     service_titan_customer_since_year = serializers.IntegerField(default=1900)
#     client_updates_client = ClientUpdateSerializer(many=True, read_only=True)

#     def get_tag(self, obj):
#         return [tag.tag for tag in obj.tag.all()]

#     class Meta:
#         model = Client
#         fields = [
#             "id",
#             "name",
#             "address",
#             "city",
#             "state",
#             "phone_number",
#             "status",
#             "service_titan_customer_since_year",
#             "service_titan_lifetime_revenue",
#             "latitude",
#             "longitude",
#             "zip_code",
#             "tag",
#             "service_titan_customer_since_year",
#             "client_updates_client"
#         ]
#         read_only_fields = fields

class ClientListSerializer(serpy.Serializer):
    id = serpy.Field()
    name = serpy.Field()
    address = serpy.Field()
    city = serpy.Field()
    state = serpy.Field()
    phone_number = serpy.Field()
    status = serpy.Field()
    service_titan_customer_since_year = serpy.Field()
    service_titan_lifetime_revenue = serpy.Field()
    latitude = serpy.Field()
    longitude = serpy.Field()
    zip_code = serpy.Field(attr='zip_code.zip_code')
    tag = serpy.MethodField()
    # client_updates_client = ClientUpdateSerializer(many=True)

    def get_tag(self, obj):
        return [tag.tag for tag in obj.tag.all()]


class ZapierClientSerializer(serializers.ModelSerializer):
    zip_code = serializers.CharField(source='zip_code.zip_code')

    class Meta:
        model = Client
        fields = (
            "name",
            "address",
            "city",
            "state",
            "zip_code",
            "phone_number",
        )
        read_only_fields = fields


class HomeListingTagsSerializer(serializers.ModelSerializer):
    class Meta:
        model = HomeListingTags
        fields = ["tag"]


class HomeListingSerializer(serializers.ModelSerializer):
    zip_code = serializers.CharField(source='zip_code.zip_code')
    tags = HomeListingTagsSerializer(many=True, read_only=True)

    class Meta:
        model = HomeListing
        fields = [
            f.name for f in HomeListing._meta.fields if f.name != "zip_code"
        ] + ["zip_code", "tags"]


class ReferralSerializer(serializers.ModelSerializer):
    enterprise = EnterpriseSerializer(required=False)
    referred_from = BasicCompanySerializer(required=True)
    referred_to = BasicCompanySerializer(required=True)
    client = ClientListSerializer()

    class Meta:
        model = Referral
        fields = [
            "contacted",
            "id",
            "enterprise",
            "referred_from",
            "referred_to",
            "client",
        ]
        read_only_fields = fields


class RealtorSerializer(serializers.ModelSerializer):
    listing_count = serializers.IntegerField(read_only=True)

    class Meta:
        model = Realtor
        fields = ('id', 'name', 'company', 'phone',
                  'email', 'url', 'listing_count')
