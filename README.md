# EDAF: An End-to-End Delay Analytics Framework for 5G-and-Beyond Networks

EDAF is a novel tool that decompose packets' end-to-end delays and determine each component's significance for optimizing the delay in 5G network.

To achieve that, EDAF
 1) modifies the OpenAirInterface 5G protocol stack by inserting numerous time measurement points across different layers,
 2) requires the application ends to report packet timestamps and sequence numbers,
 3) aggregates all time measurements to analyze the end-to-end delay, and
 4) generates insights for delay optimization.

## Requirements and preparations

For running EDAF experiments, 2 hosts and 2 USRP SDRs are required to bring up an standalone OpenAirInterface (OAI) 5G network:
1. Core Network (CN) and gNB host + gnb SDR
2. UE host + gnb SDR

They need to be connected via a secondary out-of-band wired IP network that we call it edaf-net.

Also, their clocks must be synced by running Precision Time Protocol (PTP) clock synchronization.
Therefore all hosts are supposed to be equipped with hardware timestamping capable network interface cards (NICs) which is required for PTP to sync the clocks.

Make sure Docker is installed on the hosts.

## Running an experiment

We assume EDAF server is running on a server with this IP address: `172.16.32.140`.

### 1) Run 5G Core and NLMT Server

We start by running the 5G core services on CN host by following OpenAirInterface tutorials.
Next, we need to run NLMT server inside `UPF`, `SPGWU`, or `ext-dn` container. 

NOTE1: check that it can reach EDAF server.

NOTE2: check that it can be reached from nrUE host through 5G network.

In order to do that, if running Ubuntu, you can download the applications binary and run it using these commands:
```
wget https://raw.githubusercontent.com/samiemostafavi/nlmt/master/nlmt
docker cp nlmt 5gcn-spgwu:/usr/local/bin/
docker exec -it 5gcn-spgwu chmod +x /usr/local/bin/nlmt
docker exec -d 5gcn-spgwu /bin/sh -c 'while true; do nlmt server -n 172.16.32.140:50009 -i 0 -d 0 -l 0; sleep 1; done'
```
NOTE3: change `172.16.32.140` address to your EDAF server.

### 2) Run EDAF Server

Run EDAF on the CN host by first creating a folder on the host for storing the database.
```
mkdir `pwd`/influxdbv2
```

Use the following command after creating `influxdbv2` folder on the EDAF host (CN and GNB host for instance)
```
docker run -d --rm --volume `pwd`/influxdbv2:/root/.influxdbv2 --network host  --name edaf-server samiemostafavi/edaf:latest
```
Check influxdb UI on the browser: `http://172.16.32.140:8086`

NOTE: you can check the logs via `docker logs edaf-server`

### 3) Run 5G RAN

Download and installed the modified openairinterface RAN code from our repository and checkout to develop
```
git clone https://gitlab.eurecom.fr/samiemostafavi/openairinterface5g-edaf.git
cd ~/openairinterface5g-edaf
git checkout edaf-develop
```

Build openairinterface with `--enable-edaf` for both nrUE and gNB
```
./build_oai -I
./build_oai -w USRP --ninja --gNB -C --enable-edaf
./build_oai -w USRP --ninja --nrUE -C --enable-edaf
```

To run the modified gNodeB, in the config file e.g. `gnb.sa.band78.fr1.106PRB.usrpb210.conf` you need to add EDAF server address as:
```
edaf_addr = "172.16.32.140:50015";
edaf_addr = "/tmp/edaf";
```
Then run gNodeB and check EDAF logs whether it is connected or not.

NOTE: the parameter has to be located in the same level as `Active_gNBs` and `Asn1_verbosity` as below:
```
Active_gNBs = ( "gNB-OAI");
# Asn1_verbosity, choice in: none, info, annoying
Asn1_verbosity = "none";

edaf_addr = "172.16.32.140:50015";

gNBs =
(
 {
```

For nrUE, when in the arguments passing to the execution command, you have to pass EDAF address as:
```
--edaf-addr 172.16.32.140:50011
--edaf-addr /tmp/edaf
```
Then run UE and check EDAF logs whether it is connected or not.

Upon successful connection between gNB and UE, run the traffic generator NLMT client on UE host to generate packets on uplink.

EDAF populates the influxDB, check the database's UI on port 8086 of CN+gNB host.

NOTE: if you specify a file address as above examples, it will write latseq files only without network connection.

## Run EDAF Offline

Instead of online networked mode, you can configure LATSEQ to produce `.lseq`, and NLMT to produce `.json.gz` files.
In this case you can use `offline_edaf.py` script to process the data, decompose delay, and produce a parquet file.
Pass the address of a folder to the script with the following structure:
```
FOLDER_ADDR/
-- gnb/
---- latseq.*.lseq
-- ue/
---- latseq.*.lseq
-- upf/
---- se_*.json.gz
```

For example:
```
python offline_edaf.py 240103_011728_FINAL_expB_Q1_results res.parquet
```

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

## Publications

## License

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this project except in compliance with the License. A copy of the license is included in the [LICENSE](LICENSE) file.
