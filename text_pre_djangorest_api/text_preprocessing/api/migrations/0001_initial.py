# Generated by Django 4.0.4 on 2022-05-12 15:28

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='RegexEntry',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=64, unique=True)),
                ('pattern', models.CharField(max_length=512)),
                ('replace_token', models.CharField(default=' <DEFAULT-REPLACE-TOKEN> ', max_length=64)),
            ],
        ),
    ]