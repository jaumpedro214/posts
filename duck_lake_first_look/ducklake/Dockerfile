FROM python:3.10-bookworm

WORKDIR /data

COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ["tail", "-f", "/dev/null"]