# docker build -t samiemostafavi/edaf .
# docker image push samiemostafavi/edaf

# requirements
FROM ubuntu:20.04 AS base

ARG DEBIAN_FRONTEND=noninteractive
ENV TZ="Europe/Stockholm"


RUN apt-get update &&\
    apt-get upgrade -y

RUN apt-get install software-properties-common -y
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update
RUN apt-get install python3.9 python3.9-dev python3-pip -y

RUN apt-get install -y \
    build-essential wget curl tar

RUN mkdir -p /tmp/install
WORKDIR /tmp/install

# install influxdb
RUN curl -O https://dl.influxdata.com/influxdb/releases/influxdb2-2.7.4_linux_amd64.tar.gz
RUN tar xvzf ./influxdb2-2.7.4_linux_amd64.tar.gz
RUN cp ./influxdb2-2.7.4/usr/bin/influxd /usr/local/bin/

# install influx CLI
RUN wget https://dl.influxdata.com/influxdb/releases/influxdb2-client-2.7.3-linux-amd64.tar.gz
RUN tar xvzf ./influxdb2-client-2.7.3-linux-amd64.tar.gz
RUN cp ./influx /usr/local/bin/

RUN rm -rf /tmp/install
WORKDIR /

# install edaf
COPY . /EDAF
WORKDIR /EDAF
RUN python3.9 -m pip install -U .

RUN chmod +x entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]
CMD ["python3.9","edaf.py"]