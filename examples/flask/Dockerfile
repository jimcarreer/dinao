FROM python:3.12
WORKDIR /usr/src/app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
RUN apt-get update -y
RUN apt-get install netcat-openbsd
RUN pip install --upgrade pip
RUN pip install flask==2.2.5
RUN pip install psycopg2-binary
RUN pip install gunicorn==20.1.0

COPY ./dinao-tmp/ /dinao/
RUN pip install /dinao/

COPY ./app/ /usr/src/app/
COPY ./entry.sh /usr/src/app/
RUN chmod +x /usr/src/app/entry.sh

ENTRYPOINT ["/usr/src/app/entry.sh"]
