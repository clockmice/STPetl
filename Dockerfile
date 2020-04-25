FROM python:3.7-slim-buster

ENV PYTHONUNBUFFERED 1

WORKDIR /Code

COPY requirements.txt /Code/
COPY entrypoint.sh /Code/
COPY etl /Code/etl/
COPY resources /Code/resources/

RUN python3 -m venv venv \
 && /bin/bash -c "source venv/bin/activate && pip3 install -r requirements.txt --upgrade pip"

RUN chmod +x entrypoint.sh

ENTRYPOINT ["/Code/entrypoint.sh"]