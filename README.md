# EDAF: An End-to-End Delay Analytics Framework for 5G-and-Beyond Networks

EDAF is a novel tool that decompose packets' end-to-end delays and determine each component's significance for optimizing the delay in 5G network.

To achieve that, EDAF
 1) modifies the OpenAirInterface 5G protocol stack by inserting numerous time measurement points across different layers,
 2) requires the application ends to report packet timestamps and sequence numbers,
 3) aggregates all time measurements to analyze the end-to-end delay, and
 4) generates insights for delay optimization.

## Requirements and preparations

For running EDAF experiments, 3 hosts and 2 USRP SDRs are required to bring up an standalone OpenAirInterface (OAI) 5G network:
1. Core Network (CN) host
2. gNB host and SDR
3. UE host and SDR

All hosts need to be connected via a secondary out-of-band wired IP network that we call EDAF network.

Also, their clocks must be synced by running Precision Time Protocol (PTP) clock synchronization.
Therefore all hosts are supposed to be equipped with hardware timestamping capable network interface cards (NICs) which is required for PTP to sync the clocks.

Make sure Docker is installed on the hosts.

## Running an experiment

### 1) Run 5G Core and NLMT Server

We start by running the 5G core services on CN host by following OpenAirInterface tutorials.
Next, we need to run NLMT server inside the `UPF` or `SPGWU` container.
In order to do that, we download the applications binary and run it using these commands:
```
docker exec -it 5gcn-spgwu sh -c "apt-get install wget -y"
docker exec -it 5gcn-spgwu wget https://raw.githubusercontent.com/samiemostafavi/nlmt/master/nlmt
docker exec -it 5gcn-spgwu sh -c "cp nlmt /usr/local/bin/"
docker exec -d 5gcn-spgwu /bin/sh -c 'while true; do nlmt server -n 192.168.2.2:50009 -i 0 -d 0 -l 0; sleep 1; done'
```


### 2) Run EDAF Server

Run EDAF on the CN host by first creating a folder on the host for storing the database.

Run EDAF server using the following command (create `influxdbv2` folder on the CN host beforehand):
```
docker run -d --rm --volume `pwd`/influxdbv2:/root/.influxdbv2 -p 8086:8086 -p 50009:50009 -p 50015:50015 -p 50011:50011 --name edaf samiemostafavi/edaf:latest
```
Check influxdb on the browser: `http://localhost:8086`


Run the modified OAI 5G network on gNB and UE hosts.

Upon successful connection between gNB and UE, run the traffic generator NLMT client on UE host to generate packets on uplink.

EDAF populates the influxDB, check the database's UI on port 8086 of CN host.

## Publications

## License

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this project except in compliance with the License. A copy of the license is included in the [LICENSE](LICENSE) file.
