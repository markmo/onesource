#!/usr/bin/env bash

export PYTHONPATH=.:onesource

# not allowing files to be uploaded
#gunicorn -b 0.0.0.0:5000 wsgi:app

python app.py