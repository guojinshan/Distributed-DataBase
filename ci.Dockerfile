# This is definitely not the cleanieast dockerfile but it will do the trick!
FROM python:3.9-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        make \
        git \
    && apt-get clean \
    && rm -rf /tmp/* /var/tmp/*

RUN pip install pipenv pre-commit

WORKDIR /tmp/nf26
COPY . .

RUN make init
RUN make style
RUN make test
