.PHONY: install run

SHELL := /bin/bash

install:
	virtualenv --python=python3.7 env
	source ./env/bin/activate && pip3 install -r requirements.txt

destroy:
	rm -rf env

