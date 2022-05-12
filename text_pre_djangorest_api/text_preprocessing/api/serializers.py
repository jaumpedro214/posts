from rest_framework.validators import UniqueTogetherValidator
from rest_framework import serializers
from .models import RegexEntry

class RegexMultipleSerializer(serializers.Serializer):
    names = serializers.ListField( child=serializers.CharField() )


class RegexSerializer(serializers.ModelSerializer):
    class Meta:
        model = RegexEntry
        fields = [
            'name',
            'pattern',
            'replace_token',
        ]
