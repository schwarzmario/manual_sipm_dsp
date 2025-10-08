# Manual SiPM DSP

run e.g.  
`python bin/run_dsp.py --rawdir ../data/tier/raw/lac/p14/r006 --dspdir generated/p14r006dsp --config inputs/dataprod/config/tier/dsp/l200-p14-r%-T%-SiPM-dsp_proc_chain.yaml`

Requires:
* dspeed  
* pylegendmeta  

## Troubleshooting

Re-run with only one file when you see dubious crashes in the multiprocessing mode  
If you see SEGFAULT --> rebuild the venv from scratch (probably dspeed update with incompatible [numba?] cache)
