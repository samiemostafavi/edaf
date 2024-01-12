#!/bin/bash

# Get the current timestamp in the format YYMMDD_HHMMSS
timestamp=$(date +'%y%m%d_%H%M%S')

# Create the main results folder
results_folder="${timestamp}_results"
mkdir "$results_folder"

# Create subfolders inside the results folder
mkdir "${results_folder}/gnb"
mkdir "${results_folder}/ue"
mkdir "${results_folder}/upf"

# Function to copy the latest *.lseq file from a server to the corresponding folder
copy_lseq_file() {
    local server_ip="$1"
    local user="$2"
    local password="$3"
    local destination_folder="$4"

    latest_lseq=$(sshpass -p "$password" ssh "$user@$server_ip" "ls -t /tmp/*.lseq | head -n 1")
    # Copy the latest *.lseq file using scp
    sshpass -p "$password" scp -r "$user@$server_ip:$latest_lseq" "$destination_folder"
    echo "$destination_folder/$(basename $latest_lseq)"
}

# Function to copy the latest *.json file from UPF to the corresponding folder
copy_json_file() {
    local upf_ip="$1"
    local user="$2"
    local password="$3"
    local upf_folder="$4"
    local destination_folder="$5"

    # Copy the latest *.json file using scp
    latest_json=$(sshpass -p "$password" ssh "$user@$upf_ip" "ls -t /tmp/$upf_folder/server/*.json.gz | head -n 1")
    sshpass -p "$password" scp -r "$user@$upf_ip:$latest_json" "$destination_folder"
    echo "$destination_folder/$(basename $latest_json)"
}

# Copy the latest *.lseq file from gnb (192.168.2.2) to the gnb folder
gnb_file=$(copy_lseq_file "192.168.2.2" "wlab" "wlab" "${results_folder}/gnb")
echo "Copied gnb file: $gnb_file"

# Copy the latest *.lseq file from ue (192.168.1.1) to the ue folder
ue_file=$(copy_lseq_file "192.168.1.1" "wlab" "wlab" "${results_folder}/ue")
echo "Copied ue file: $ue_file"

# Copy the latest *.json file from UPF (192.168.2.3) to the UPF folder
upf_file=$(copy_json_file "192.168.2.3" "wlab" "wlab" "m1/fingolfin" "${results_folder}/upf")
echo "Copied UPF file: $upf_file"


python tools/rdtsctots.py "$gnb_file" > "$results_folder/gnb/gnb.lseq"
python ul_postprocess_gnb.py "$results_folder/gnb/gnb.lseq" > "$results_folder/gnb/gnbjourneys.json"
echo "Created gnbjourneys.json"

python tools/rdtsctots.py "$ue_file" > "$results_folder/ue/nrue_tmp.lseq"
tac "$results_folder/ue/nrue_tmp.lseq" > "$results_folder/ue/nrue.lseq"
python ul_postprocess_nrue.py "$results_folder/ue/nrue.lseq" > "$results_folder/ue/nruejourneys.json"
echo "Created nruejourneys.json"

python ul_combine.py "$results_folder/gnb/gnbjourneys.json" "$results_folder/ue/nruejourneys.json" "$upf_file" "$results_folder/journeys.parquet"
echo "Created journeys.parquet"

# Process and decompose latency
python ul_decompose_plot_v2.py "$results_folder/journeys.parquet" "$results_folder"
python ul_time_plot_v2.py "$results_folder/journeys.parquet" "$results_folder"
echo "Created plots"