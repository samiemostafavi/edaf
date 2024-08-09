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
