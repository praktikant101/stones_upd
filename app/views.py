from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response

from drf_yasg.utils import swagger_auto_schema

from .serializers import FileUploadSerializer
from .service import process_data


class FileUploadView(APIView):

    parser_classes = (MultiPartParser, FormParser)

    @swagger_auto_schema(request_body=FileUploadSerializer)
    def post(self, request, format=None):
        serializer = FileUploadSerializer(data=request.data)
        if serializer.is_valid(raise_exception=True):
            return Response(process_data(serializer.validated_data['file']), status=200)