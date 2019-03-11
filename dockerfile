# base image
FROM python:3.6.8-slim

# install netcat
RUN apt-get update && \
    apt-get -y install netcat gcc && \
    apt-get -y install --reinstall build-essential && \
    apt-get clean

# set working directory
WORKDIR /usr/src/app

# add and install requirements
COPY ./requirements.txt /usr/src/app/requirements.txt
RUN pip install -r requirements.txt

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

# add app
COPY config/ /usr/src/app/config/
COPY onesource/ /usr/src/app/onesource/
COPY .env /usr/src/app
COPY models /usr/src/app/models/
COPY resources /usr/src/app/resources/
COPY ./app.py /usr/src/app
COPY ./wsgi.py /usr/src/app

# run server
CMD ["/usr/src/app/entrypoint.sh"]
