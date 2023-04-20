from django.urls import path

from .views import FileUploadView

urlpatterns = [
    path("file/", FileUploadView.as_view(), name="file")
]