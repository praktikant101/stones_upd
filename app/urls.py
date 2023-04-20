from django.urls import path

from .views import FileUploadView, ClientGemsView, ClientView

urlpatterns = [
    path("file/", FileUploadView.as_view(), name="file"),
    path("client-gems/", ClientGemsView.as_view(), name="client"),
    path("client/", ClientView.as_view(), name="client"),
]