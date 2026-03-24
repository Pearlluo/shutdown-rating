#!/bin/bash
gunicorn --bind=0.0.0.0:$PORT Web:app