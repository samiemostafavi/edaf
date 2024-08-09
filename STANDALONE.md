# Run EDAF Standalone

## Run EDAF Standalone

If you are interested in using EDAF over an arbitrary network link, and not OpenAirInterface, follow this section.
In such a scenario, you will lose the decomposition capability and you can only analyze the end-to-end delay.
The hosts are required to be clock synchronized.
For running standalone EDAF experiments, 2 hosts are required:
1. Server host
2. Client host

Follow the steps below to run EDAF in standalone configuration.

Create a docker network to run server services on it:
```
docker network create --driver=bridge --subnet=10.89.89.0/24 --ip-range=10.89.89.0/24 --gateway=10.89.89.1 edaf-net
```

### 1) Run EDAF Server

Run EDAF on the server host by first creating a folder on the host for storing the database.

Use the following command after creating `influxdbv2` folder on the server host:
```
docker run -e STANDALONE=true -d --rm --volume `pwd`/influxdbv2:/root/.influxdbv2 --network edaf-net --ip 10.89.89.2 -p 0.0.0.0:8086:8086 --name edaf samiemostafavi/edaf:latest
```
Check influxdb UI on the browser: `http://192.168.2.2:8086`

### 2) Run NLMT Server

Run NLMT server
```
docker run -d --rm --name nlmt-server --network edaf-net --ip 10.89.89.3 -p 0.0.0.0:2112:2112 -p 0.0.0.0:2112:2112/udp samiemostafavi/nlmt /bin/sh -c 'while true; do nlmt server -n 10.89.89.2:50009 -i 0 -d 0 -l 0; sleep 1; done'
```

### 3) Run NLMT Client

On the client host, run nlmt client towards the server:
```
./nlmt client --tripm=oneway -i 10ms -f 5ms -g edaf1/fingolfin -l 500 -m 1 -d 5m -o d --outdir=/tmp/ 192.168.2.2
```
