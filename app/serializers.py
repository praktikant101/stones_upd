from rest_framework import serializers

from .models import Client


class FileUploadSerializer(serializers.Serializer):
    file = serializers.FileField()


class ClientGemsSerializer(serializers.ModelSerializer):
    class Meta:
        model = Client
        fields = "__all__"
