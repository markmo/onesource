# base image
FROM python:3.6.8-slim

# https://bugs.debian.org/cgi-bin/bugreport.cgi?bug=863199
RUN mkdir -p /usr/share/man/man1

# install netcat
RUN echo "deb http://archive.debian.org/debian stretch main" > /etc/apt/sources.list && \
    apt-get update && \
    apt-get -y install netcat gcc libpq-dev openjdk-8-jdk && \
    apt-get -y install --reinstall build-essential && \
    apt-get clean

# set working directory
WORKDIR /usr/src/app

# add and install requirements
COPY ./requirements.txt /usr/src/app/requirements.txt
RUN pip install -r requirements.txt
RUN python -m spacy download en_core_web_sm
RUN python -m spacy download en

# add and install libraries
COPY ./libs/relation_extraction-0.1-py3-none-any.whl /usr/src/app/relation_extraction-0.1-py3-none-any.whl
RUN pip install relation_extraction-0.1-py3-none-any.whl

# add entrypoint.sh
COPY ./entrypoint.sh /usr/src/app/entrypoint.sh
RUN chmod +x /usr/src/app/entrypoint.sh

# make directories
RUN mkdir onesource
RUN mkdir config
RUN mkdir resources
RUN mkdir models
RUN mkdir -p /var/data/in
RUN mkdir -p /var/data/out
RUN mkdir -p /var/data/temp

# add app
COPY config/ /usr/src/app/config/
COPY onesource/ /usr/src/app/onesource/
COPY .env /usr/src/app
# COPY models /usr/src/app/models/
# COPY resources /usr/src/app/resources/
COPY ./app.py /usr/src/app
COPY ./wsgi.py /usr/src/app

# run server
CMD ["/usr/src/app/entrypoint.sh"]
