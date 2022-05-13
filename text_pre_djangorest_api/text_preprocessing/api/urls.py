from django.urls import path
from . import views


urlpatterns = [
    path("regex/<str:name>/", views.RegexRetrieveUpdateAPIView.as_view()),
    path("regex/", views.RegexCreateAPIView.as_view()),
    path("process/", views.RequestProcessingAPIView.as_view()),
]
