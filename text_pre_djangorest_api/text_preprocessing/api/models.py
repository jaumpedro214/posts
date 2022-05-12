from django.db import models


class RegexEntry(models.Model):
    name = models.CharField(max_length=64, unique=True, blank=False, primary_key=True)
    pattern = models.CharField(max_length=512, blank=False)
    replace_token = models.CharField(max_length=64, default=" <DEFAULT-REPLACE-TOKEN> ")
