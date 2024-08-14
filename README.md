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
2. nrUE host + nrUE SDR

They need to be connected via a secondary out-of-band wired IP network.

Also, their clocks must be synced by running Precision Time Protocol (PTP) clock synchronization.
Therefore all hosts are supposed to be equipped with hardware timestamping capable network interface cards (NICs) which is required for PTP to sync the clocks.

Make sure Docker is installed on the hosts.

## Running Online EDAF

In this case, we process the data online and can observe the delay decomposition live. However due to the complexity involved in live data processing and pipeline, some packets (~30%) maye be ignored.

In the online scenario we run a server application (EDAF server) using docker which is assumed to be on a server with this IP address: `172.16.32.140`.

gNB and nrUE hosts should be able to reach this server (be pingable) on a network other than 5G network.

Follow the steps below:

### 1) Run 5G Core and NLMT Server

We start by running the 5G core services on CN host by following OpenAirInterface tutorials.

Next, we need to run NLMT server in an environment accessible via 5G's UPF container. For instance inside UPF container is an option, as well as `ext-dn` container.

NOTE1: check that NLMT server can reach EDAF server (ping `172.16.32.140`).

NOTE2: after the 5G connection was established, check that NLMT server can be reached from nrUE host through 5G network.

In order to do that, if running Ubuntu, you can download the application's binary and run it using these commands:
```
wget https://raw.githubusercontent.com/samiemostafavi/nlmt/master/nlmt
docker cp nlmt oai-ext-dn:/usr/local/bin/
docker exec -it oai-ext-dn chmod +x /usr/local/bin/nlmt
docker exec -d oai-ext-dn /bin/sh -c 'while true; do nlmt server -n 172.16.32.140:50009 -i 0 -d 0 -l 0; sleep 1; done'
```
NOTE3: change `172.16.32.140` address to your EDAF server.

### 2) Run EDAF Server

In the machine with IP address `172.16.32.140`, run EDAF server by first creating a folder on the host for storing the database.
```
mkdir `pwd`/influxdbv2
```

Then Use the following command to bring up EDAF server in a container
```
docker run -d --rm --volume `pwd`/influxdbv2:/root/.influxdbv2 --network host  --name edaf-server samiemostafavi/edaf:latest
```
Now you can check influxdb UI on the browser: `http://172.16.32.140:8086`. By default, login username is `edaf`, password is `4c5f28e30698bf883e18193`.

NOTE: also you can check EDAF server logs via `docker logs edaf-server`

### 3) Run 5G RAN

Download and install the modified openairinterface RAN code from our repository and checkout to `edaf-develop`
```
git clone https://gitlab.eurecom.fr/samiemostafavi/openairinterface5g-edaf.git
cd ~/openairinterface5g-edaf
git checkout edaf-develop
```

Build openairinterface with `--enable-edaf` for both nrUE and gNB (use openairinterface instructions for more accurate instructions)
```
./build_oai -I
./build_oai -w USRP --ninja --gNB -C --enable-edaf
./build_oai -w USRP --ninja --nrUE -C --enable-edaf
```

To run the modified gNodeB, in the config file e.g. `gnb.sa.band78.fr1.106PRB.usrpb210.conf` you need to add EDAF server address as:
```
edaf_addr = "172.16.32.140:50015";
```
Then run gNodeB and check EDAF server logs whether it is connected or not.

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
```

Then run nrUE to connect to oai 5G network. Check EDAF server logs to see whether it is connected or not.

### 4) Run NLMT client

On the nrUE machine, after the connection to the 5G network is established and you got IP, you must run NLMT client to produce periodic traffic towards the NLMT server.
NLMT client does not communicate with EDAF. 
It's purpose is only producing traffic for NLMT server.
NOTE: check first that you can ping the NLMT server machine (UPF, or ext-dn)

Download and run NLMT client as
```
wget https://raw.githubusercontent.com/samiemostafavi/nlmt/master/nlmt
chmod +x /usr/local/bin/nlmt
 ./nlmt client --tripm=oneway -i 10ms -f 5ms -g edaf1/test -l 500 -m 1 -d 5m -o d --outdir=/tmp/ <NLMTSERVER_IP>
```

After you saw 'connection established' here, EDAF server starts populating the influxDB, so you can check the database's UI and observe or download the data. 

## Run EDAF Offline

Instead of online networked mode, you can have a setup that works only with files. This requires running the `offline_edaf.py` python script so first setup a python 3.9 or higher environment and install the requirements:
```
git clone https://github.com/samiemostafavi/edaf.git
cd ~/edaf
python3 -m virtualenv ./venv
source venv/bin/activate
pip install -Ur requirements.txt
```

NOTE: Python version must be greater or eqaul than 3.9

### 1) Run 5G Core and NLMT Server

We start by running the 5G core services on CN host by following OpenAirInterface tutorials.

Next, we need to run NLMT server in an environment accessible via 5G's UPF container. For instance inside UPF container is an option, as well as `ext-dn` container.

NOTE: after the 5G connection was established, check that NLMT server can be reached from nrUE host through 5G network.

In order to do that, if running Ubuntu, you can download the application's binary and run it using these commands:
```
wget https://raw.githubusercontent.com/samiemostafavi/nlmt/master/nlmt
docker cp nlmt oai-ext-dn:/usr/local/bin/
docker exec -it oai-ext-dn chmod +x /usr/local/bin/nlmt
docker exec -d oai-ext-dn /bin/sh -c 'while true; do nlmt server -o d --outdir=/tmp/ -i 0 -l 0; sleep 1; done'
```

Note that we configure NLMT server here to produce `.json.gz` files (without `-n`):
```
./nlmt server -o d --outdir=/tmp/ -i 0 -l 0
```

This will create a folder inside `/tmp/`, with the address that you specify on the NLMT client side in argument `-g`. 
For example with this NLMT client command:
```
./nlmt client --tripm=oneway -i 10ms -g edaf/test -l 100 -m 1 -d 5m -o d --outdir=/tmp/ 192.168.70.129
```
A `.json.gz` file will be created with an autogenerated name in `/tmp/edaf/test/server` folder on the NLMT server machine.

As you can see the NLMT client command does not differ compared to the online EDAF.

### Run 5G RAN

Download and install the modified openairinterface RAN code from our repository and checkout to `edaf-develop`
```
git clone https://gitlab.eurecom.fr/samiemostafavi/openairinterface5g-edaf.git
cd ~/openairinterface5g-edaf
git checkout edaf-develop
```

Build openairinterface with `--enable-edaf` for both nrUE and gNB (use openairinterface instructions for more accurate instructions)
```
./build_oai -I
./build_oai -w USRP --ninja --gNB -C --enable-edaf
./build_oai -w USRP --ninja --nrUE -C --enable-edaf
```

In this case, we configure gNB and nrUE to produce `.lseq` files

ُThis can be done on the RAN executables by setting a folder address instead of previously mentioned edaf IP:port address:
- On gnb use `edaf_addr = "/tmp/edaf";` instead of `edaf_addr = "172.16.32.140:50015";` inside the gnb conf file.
- On nrUE command line arguments, use `--edaf-addr /tmp/edaf` instead of `--edaf-addr 172.16.32.140:50011`

After running the experiment you will have 3 files: 2 `*.lseq` files and a `se_*.json.gz` file.

### Run EDAF Script

We process these files via `offline_edaf.py`.

To use `offline_edaf.py` script to process the data, decompose delay, and produce a parquet database file,
The files need to be placed in a folder with this structure:
```
FOLDER_ADDR/
-- gnb/
---- latseq.*.lseq
-- ue/
---- latseq.*.lseq
-- upf/
---- se_*.json.gz
```

Then the folder address must be passed to the script:
```
python offline_edaf.py FOLDER_ADDR res.parquet
```
This will create a `res.parquet` file next to the script.

If you have ssh access to all hosts, you can use the script `download_offline_files.sh` to copy the latest files to your machine. Remember to modify the script with correct IP addresses, ssh credentials, and file addresses.


## Paper and Citation
This repository contains the approach to analyze 5G end-to-end delay in this paper 

***EDAF: An End-to-End Delay Analytics Framework for 5G-and-Beyond Networks*** [here](https://arxiv.org/abs/2401.09856)

If you use this work in your research, please cite our paper
```
@misc{mostafavi2024edafendtoenddelayanalytics,
      title={EDAF: An End-to-End Delay Analytics Framework for 5G-and-Beyond Networks}, 
      author={Samie Mostafavi and Marius Tillner and Gourav Prateek Sharma and James Gross},
      year={2024},
      eprint={2401.09856},
      archivePrefix={arXiv},
      primaryClass={cs.NI},
      url={https://arxiv.org/abs/2401.09856}, 
}
```

## License

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this project except in compliance with the License. A copy of the license is included in the [LICENSE](LICENSE) file.
