from rest_framework import serializers
from .models import RegexEntry


class RegexSerializer(serializers.ModelSerializer):
    class Meta:
        model = RegexEntry
        fields = [
            'name',
            'pattern',
            'replace_token'
        ]
