from django.urls import path, include
from rest_framework import routers
from . import views

router = routers.DefaultRouter()

client_patterns = [
    path(
        "<str:user>/",
        views.ClientListView.as_view(),
        name="client-list",
    ),
    path(
        "",
        views.ClientListView.as_view(),
        name="save-client-filter",
    ),
]

urlpatterns = [
    path(
        "recentlysold/<str:company>/",
        views.RecentlySoldView.as_view(),
        name="recently-sold",
    ),
    path(
        "downloadrecentlysold/<str:company>/",
        views.AllRecentlySoldView.as_view(),
        name="all-recently-sold",
    ),
    path(
        "forsale/<str:company>/",
        views.ForSaleView.as_view(),
        name="for-sale",
    ),
    path(
        "downloadforsale/<str:company>/",
        views.AllForSaleView.as_view(),
        name="all-for-sale",
    ),
    path(
        "update/<str:company>/",
        views.UpdateStatusView.as_view(),
        name="update-status",
    ),
    path(
        "updateclient/", views.UpdateClientView.as_view(), name="updateclient"
    ),
    path(
        "upload/<str:company>/",
        views.UploadFileView.as_view(),
        name="upload-file",
    ),
    path(
        "downloadclients/<str:user>/",
        views.DownloadClientView.as_view(),
        name="all-client-list",
    ),
    path("clients/", include(client_patterns)),
    path(
        "salesforce/<str:company>/",
        views.SalesforceConsumerView.as_view(),
        name="salesforce-consumer",
    ),
    path(
        "servicetitan/<str:company>/",
        views.ServiceTitanView.as_view(),
        name="servicetitian",
    ),
]
