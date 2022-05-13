from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.generics import RetrieveUpdateDestroyAPIView, CreateAPIView
from rest_framework.serializers import ValidationError

from .models import RegexEntry
from .serializers import (RegexProcessingResponseSerializer, RegexSerializer,
                          RegexProcessingRequestSerializer
                          )

from .regex.apply import RegexApplier


class RegexCreateAPIView(CreateAPIView):
    queryset = RegexEntry.objects.all()
    serializer_class = RegexSerializer
    lookup_field = "name"


class RegexRetrieveUpdateAPIView(RetrieveUpdateDestroyAPIView):
    queryset = RegexEntry.objects.all()
    serializer_class = RegexSerializer
    lookup_field = "name"


class RequestProcessingAPIView(APIView):
    def get(self, request):
        serializer = RegexProcessingRequestSerializer(data=request.data)

        if not serializer.is_valid(raise_exception=True):
            pass

        instances = []
        text = serializer.data['text']
        for regex_name in serializer.data['steps']:
            try:
                instance = RegexEntry.objects.get(name=regex_name)
            except RegexEntry.DoesNotExist:
                raise ValidationError(
                    detail=f"Regex {regex_name} don't exist",
                    code=404
                )
            instances.append(instance)

        instances_as_dict = RegexSerializer(instances, many=True).data
        processed_text = RegexApplier(instances_as_dict).replace(text)
        response = {'text': text,
                    'processed_text': processed_text}
        return Response(response)
