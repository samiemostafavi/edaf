# latseq-pp
Python package to post process Latseq measurements from Openairinterface5G


## gnb
```
python tools/rdtsctots.py 2_results/latseq.30102023_152306.lseq > 2_results/gnb.lseq
python postprocess_gnb.py 2_results/gnb.lseq > 2_results/gnbjourneys.json
```

## nrue

```
python tools/rdtsctots.py 2_results/latseq.30102023_152306.lseq > 2_results/nrue_tmp.lseq
tac 2_results/nrue_tmp.lseq > 2_results/nrue.lseq
python postprocess_nrue.py 2_results/nrue.lseq > 2_results/nruejourneys.json
```