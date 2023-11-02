# latseq-pp
Python package to post process Latseq measurements from Openairinterface5G


## Convert timestamps

```
python tools/rdtsctots.py latseq.30102023_152306.lseq > 5gsa.lseq
```

Modify the global variables in `latseq_logs.py` according to the measurement points etc.
```
S_TO_MS = 1000
KWS_BUFFER = ['tx', 'rx', 'retx']  # buffer keywords
KWS_NO_CONCATENATION = ['pdcp.in']  # TODO
KWS_IN_D = ['ip.in']  # TODO : put in conf file and verify why when add 'ip' it breaks rebuild
KWS_OUT_D = ['phy.out.proc']
KWS_IN_U = ['phy.start']
KWS_OUT_U = ['gtp.out']
VERBOSITY = False  # Verbosity for rebuild phase False by default
```

To check the paths
```
python ./tools/latseq_logs.py -r -l 5gsa.lseq
```

To check the points
```
python ./tools/latseq_logs.py -p -l 5gsa.lseq
```

Create the journeys by running this command. The results will be saved to .pkl file. This step is necessary.
```
python ./tools/latseq_logs.py -j -l 5gsa.lseq -v
```

Create .lseqj file
```
python3 tools/latseq_logs.py -o -l 5gsa5.lseq > 5gsa5.lseqj 2>/dev/null
```