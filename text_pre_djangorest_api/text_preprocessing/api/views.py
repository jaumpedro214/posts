from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.generics import RetrieveUpdateAPIView, CreateAPIView

from .models import RegexEntry
from .serializers import RegexSerializer


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
        pass
