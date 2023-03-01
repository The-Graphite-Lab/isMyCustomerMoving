from django.contrib import admin

# Register your models here.
from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from django.utils.translation import gettext as _
from .models import CustomUser, Company, InviteToken, Franchise


class CustomUserAdmin(UserAdmin):
    """Define admin model for custom User model with no username field."""
    fieldsets = (
        (None, {'fields': ('first_name',
                           'last_name',
                           'status',
                           'email',
                           'phone',    
                           'company',
                           'isVerified',
                           'otp_enabled',
                            'otp_base32',
                            'otp_auth_url',  
                                                  
                           )}),
        (_('Permissions'), {'fields': ('is_active', 'is_staff',
         'is_superuser', 'groups', 'user_permissions')}),
        (_('Important dates'), {'fields': ('last_login', 'date_joined')}),
    )
    add_fieldsets = (
        (None, {
            'classes': ('wide',),
            'fields': ('first_name', 'last_name', 'is_staff', 'password1', 'password2'),
        }),
    )

    list_display = ('first_name', 'last_name', 'email',
                    'isVerified', 'company__name')
    search_fields = ('id', 'first_name', 'last_name', 'email')
    ordering = ('id',)
    list_filter = ('is_staff', 'isVerified',)

    def company__name(self, obj):
        return obj.company.name

class CompanyAdmin(admin.ModelAdmin):
    list_display = ('name', 'accessToken', 'franchise')

# class ClientAdmin(admin.ModelAdmin):
#     list_display = ('name', 'address', 'status', 'city', 'state', 'contacted', 'note', 'zipCode', 'company__name', 'servTitanID', 'phoneNumber')
#     search_fields = ('name', 'address', 'status', 'city', 'state', 'servTitanID', 'zipCode__zipCode', 'company__name')

#     # def zipCode__zip(self, obj):
#     #     return obj.zipCode.zipCode

#     def company__name(self, obj):
#         return obj.company.name

# class ClientUpdateAdmin(admin.ModelAdmin):
#     list_display = ('id', 'client__name', 'company__name', 'status', 'listed')
#     search_fields = ('id', 'client__name', 'status', 'listed', 'client__company__name')

#     def client__name(self, obj):
#         return obj.client.name

#     def company__name(self, obj):
#         return obj.client.company.name

# class ZipcodeAdmin(admin.ModelAdmin):
#     list_display = ('zipCode', 'lastUpdated', 'count')
#     search_fields = ['zipCode', 'lastUpdated']

#     def count(self, obj):
#         return Client.objects.filter(zipCode=obj.zipCode).count()

# class HomeListingAdmin(admin.ModelAdmin):
#     list_display = ('address', 'zipCode', 'status', 'listed', 'ScrapeResponse')
#     search_fields = ['address', 'status', 'zipCode__zipCode',]

    # def zipCode__zip(self, obj):
    #     return obj.zipCode.zipCode

class InviteTokenAdmin(admin.ModelAdmin):
    list_display = ('id', 'email', 'company')
    search_fields = ['id', 'email', 'company']

# class ProgressUpdateAdmin(admin.ModelAdmin):
#     list_display = ('id', 'percentDone')

# class TaskAdmin(admin.ModelAdmin):
#     list_display = ('id', 'complete', 'updater', 'deleted')

# class ScrapeResponseAdmin(admin.ModelAdmin):
#     list_display = ('id', 'date', 'zip', 'status', 'url')
#     search_fields = ['id', 'date', 'zip', 'status', 'url']

class FranchiseAdmin(admin.ModelAdmin):
    list_display = ('name', 'mainCompany')
    search_fields = ['name', 'mainCompany']

# class ReferralAdmin(admin.ModelAdmin):
#     # all fields
#     list_display = ('id', 'franchise', 'referredFrom', 'referredTo', 'client')
#     search_fields = ['id', 'franchise', 'referredFrom', 'referredTo', 'client']

# Register your models here.
# admin.site.register(HomeListing, HomeListingAdmin)
# admin.site.register(ZipCode, ZipcodeAdmin)
# admin.site.register(Client, ClientAdmin)
admin.site.register(Company, CompanyAdmin)
admin.site.register(CustomUser, CustomUserAdmin)
admin.site.register(InviteToken, InviteTokenAdmin)
# admin.site.register(ClientUpdate, ClientUpdateAdmin)
# admin.site.register(ScrapeResponse, ScrapeResponseAdmin)
admin.site.register(Franchise, FranchiseAdmin)
# admin.site.register(Referral, ReferralAdmin)
