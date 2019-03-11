#!/usr/bin/env bash

export PYTHONPATH=.:onesource
gunicorn -b 0.0.0.0:6000 wsgi:app