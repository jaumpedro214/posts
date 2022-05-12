from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.generics import RetrieveUpdateAPIView, CreateAPIView
from rest_framework.serializers import ValidationError

from .models import RegexEntry
from .serializers import RegexSerializer, RegexMultipleSerializer


class RegexCreateAPIView(CreateAPIView):
    queryset = RegexEntry.objects.all()
    serializer_class = RegexSerializer
    lookup_field = "name"


class RegexRetrieveUpdateAPIView(RetrieveUpdateAPIView):
    queryset = RegexEntry.objects.all()
    serializer_class = RegexSerializer
    lookup_field = "name"


class RegexMultipleAPIView(APIView):
    def get(self, request):
        serializer = RegexMultipleSerializer(data=request.data)

        if not serializer.is_valid(raise_exception=True):
            pass

        instances = []
        for regex_name in serializer.data['names']:
            try:
                instance = RegexEntry.objects.get(name=regex_name)
            except RegexEntry.DoesNotExist:
                raise ValidationError(
                    detail=f"Regex {regex_name} don't exist",
                    code=404
                )
            instances.append(instance)

        return Response(RegexSerializer(instances, many=True).data)
