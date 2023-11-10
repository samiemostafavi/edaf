# latseq-pp
Python package to post process Latseq measurements from Openairinterface5G

Create a folder and name it using the `*_results` format. Place the openairinterface's `*.lseq` and irtt's `*.json` files in it.

## Uplink latency sequencing

The results folder in the examples below is named `2_results`.

1. Process gNB lseq file:
    ```
    python tools/rdtsctots.py 2_results/latseq.30102023_152306.lseq > 2_results/gnb.lseq
    python ul_postprocess_gnb.py 2_results/gnb.lseq > 2_results/gnbjourneys.json
    ```

2. Process UE lseq file:
    ```
    python tools/rdtsctots.py 2_results/latseq.30102023_152306.lseq > 2_results/nrue_tmp.lseq
    tac 2_results/nrue_tmp.lseq > 2_results/nrue.lseq
    python ul_postprocess_nrue.py 2_results/nrue.lseq > 2_results/nruejourneys.json
    ```

3. Combine `json` files and produce `parquet` file using `ul_parser.ipynb`

4. Create RAN latency, TX latency, queuing delays, etc:
    ```
    python ul_plot.py 2_results/journeys.parquet 2_results/res.png
    ```