from rest_framework import serializers
from .models import RegexEntry


class RegexProcessingRequestSerializer(serializers.Serializer):
    steps = serializers.ListField(child=serializers.CharField())
    text = serializers.CharField()


class RegexProcessingResponseSerializer(serializers.Serializer):
    text = serializers.CharField()
    processed_text = serializers.CharField()


class RegexSerializer(serializers.ModelSerializer):
    class Meta:
        model = RegexEntry
        fields = [
            'name',
            'pattern',
            'replace_token',
        ]
