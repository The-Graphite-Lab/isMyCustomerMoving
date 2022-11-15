from django.urls import path, include
from rest_framework import routers
from . import views

from rest_framework_simplejwt.views import (
    TokenRefreshView,
)

router = routers.DefaultRouter()
router.register(r"users", views.UserViewSet)

urlpatterns = [
    # path('reset/', views.ResetView, name='reset'),
    path('reset_request/', views.ResetRequestView.as_view(), name='reset_request'),
    path('update/<str:company>/', views.UpdateStatusView.as_view(), name='update-status'),
    path('clients/<str:company>/', views.ClientListView.as_view(), name='client-list'),
    path('manageuser/<str:company>/', views.ManageUserView.as_view(), name='manageuser'),
    path('createCompany/', views.createCompany, name='createCompany'),
    path('updatenote/<str:company>/', views.UpdateNoteView.as_view(), name='updatenote'),
    path('contacted/<str:company>/', views.UpdateContactedView.as_view(), name='updatecontacted'),
    path('deleteClient/<str:company>/', views.DeleteClientView.as_view(), name='deleteclient'),
    path('upload/', views.UploadFileView.as_view(), name='upload-file'),
    path('register/', views.RegisterView.as_view(), name='register'),
    path('login/', views.MyTokenObtainPairView.as_view(), name='login'),
    path('refresh_token/', TokenRefreshView.as_view(), name='refresh_token'),
    path('confirmation/<str:pk>/<str:uid>/', views.confirmation, name='email_confirmation'),
    path("", include(router.urls)),
    
]
