
apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y \
    --no-install-recommends \
    apt-utils \
    software-properties-common \
    build-essential \
    pkg-config screen git wget curl tar vim

add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install python3.9 python3.9-dev python3-pip -y

mkdir -p /tmp/install \
    && cd /tmp/install

curl -O https://dl.influxdata.com/influxdb/releases/influxdb2-2.7.4_linux_amd64.tar.gz \
    && tar xvzf ./influxdb2-2.7.4_linux_amd64.tar.gz \
    && cp ./influxdb2-2.7.4/usr/bin/influxd /usr/local/bin/

wget https://dl.influxdata.com/influxdb/releases/influxdb2-client-2.7.3-linux-amd64.tar.gz \
    && tar xvzf ./influxdb2-client-2.7.3-linux-amd64.tar.gz \
    && cp ./influx /usr/local/bin/

cd / && rm -rf /tmp/install

git clone https://github.com/samiemostafavi/edaf.git && mv edaf EDAF && cd EDAF
python3.9 -m pip install -U .

# run influxd in a screen session
screen -S influxd
influxd
# exit the screen by pressing Ctrl-A and D

influx setup \
  --username edaf \
  --password 4c5f28e30698bf883e18193 \
  --org expeca \
  --bucket latency \
  --force

influx auth list --json > influx_auth.json

# run edaf standalone in a screen session
screen -S edaf
cd /EDAF/ && STANDALONE=true python3.9 edaf.py

cd / && wget https://raw.githubusercontent.com/samiemostafavi/nlmt/master/nlmt \
    && cp nlmt /usr/local/bin/ \
    && chmod +x /usr/local/bin/nlmt \
    && rm nlmt
    
# run nlmt in a screen session
screen -S nlmt
while true; do nlmt server -n localhost:50009 -i 0 -d 0 -l 0; sleep 1; done
# exit the screen by pressing Ctrl-A and D

# prediction app installation
cd / && git clone https://github.com/samiemostafavi/wireless-pr3d.git \
    && cd wireless-pr3d/ \
    && git checkout develop \
    && cd demo
    
python3.9 -m pip install virtualenv \ 
    && python3.9 -m virtualenv ./venv \
    && source venv/bin/activate \
    && pip install -Ur requirements.txt
    && deactivate

# copy the token and paste it in conf.json
cat /EDAF/influx_auth.json
vim /wireless-pr3d/demo/conf.json
# run pr3d in a screen session
screen -S pr3d
cd /wireless-pr3d/demo && source venv/bin/activate
python main.py
