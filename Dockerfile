FROM python:latest

RUN mkdir -p /usr/src/
WORKDIR /usr/src/

COPY . .
RUN pip install -r requirements.txt
