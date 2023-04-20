from rest_framework import generics
from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response

from drf_yasg.utils import swagger_auto_schema

from .models import Client
from .serializers import FileUploadSerializer, ClientGemsSerializer
from .service import process_data


class FileUploadView(APIView):
    parser_classes = (MultiPartParser, FormParser)

    @swagger_auto_schema(request_body=FileUploadSerializer)
    def post(self, request, format=None):
        serializer = FileUploadSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            result = process_data(serializer.validated_data['file'])
            return Response({"status": result["Status"], "outcome": result["Outcome"]}, status=200)


class ClientGemsView(generics.ListAPIView):
    queryset = Client.objects.filter(gems__gt=[])
    serializer_class = ClientGemsSerializer


class ClientView(generics.ListAPIView):
    queryset = Client.objects.all()
    serializer_class = ClientGemsSerializer
