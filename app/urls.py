from django.urls import path
from django.views.decorators.cache import cache_page

from .views import FileUploadView, ClientGemsView, ClientViewTopFive

urlpatterns = [
    path("file/", FileUploadView.as_view(), name="file"),
    path("client-gems/", cache_page(60*15)(ClientGemsView.as_view()), name="client"),
    path("client-top-five/", cache_page(60*15)(ClientViewTopFive.as_view()), name="client"),
]