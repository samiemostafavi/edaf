# EDAF: An End-to-End Delay Analytics Framework for 5G-and-Beyond Networks

Python package to post process Latseq measurements from Openairinterface5G

Create a folder and name it using the `*_results` format. Place the openairinterface's `*.lseq` and irtt's `*.json` files in it.

## Uplink latency sequencing

The results folder in the examples below is named `6_results`.

1. Process gNB lseq file:
    ```
    python tools/rdtsctots.py 6_results/gnb/latseq.30102023_152306.lseq > 6_results/gnb/gnb.lseq
    python ul_postprocess_gnb.py 6_results/gnb/gnb.lseq > 6_results/gnb/gnbjourneys.json
    ```

2. Process UE lseq file:
    ```
    python tools/rdtsctots.py 6_results/ue/latseq.30102023_152306.lseq > 6_results/ue/nrue_tmp.lseq
    tac 6_results/ue/nrue_tmp.lseq > 6_results/ue/nrue.lseq
    python ul_postprocess_nrue.py 6_results/ue/nrue.lseq > 6_results/ue/nruejourneys.json
    ```

3. Combine `json` files and produce `parquet` file using:
    ```
    python ul_combine.py 6_results/gnb/gnbjourneys.json 6_results/ue/nruejourneys.json 6_results/upf/se_12-1-1-2_59708_20231116_104859.json.gz 6_results/journeys.parquet
    ```

4. Process and decompose latency:
    ```
    python ul_decompose_plot.py 6_results/journeys.parquet 6_results
    python ul_time_plot.py 6_results/journeys.parquet 6_results
    ```

## License

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this project except in compliance with the License. A copy of the license is included in the [LICENSE](LICENSE) file.
