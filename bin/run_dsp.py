import pandas as pd
import numpy as np
import os, json
import pint
from lgdo.lh5 import read, read_as, ls, show
from dspeed import build_dsp
import argparse
from glob import glob
from concurrent.futures import ProcessPoolExecutor
from functools import partial

u = pint.get_application_registry()

class Task:
    def __init__(self, config_file):
        self.config_file = config_file

    def run_single(self, raw_file: str, dsp_file: str, *, force: bool) -> bool:
        if not force and os.path.exists(dsp_file):
            return False
        print("Calling build_dsp for", raw_file, "->", dsp_file, "with config", self.config_file)
        build_dsp(
            raw_file,
            dsp_file,
            dsp_config=self.config_file,
            lh5_tables=["S055"],
            write_mode="r"  # used to overwrite an existing file
        )
        return True
    
    #stupid python does not want to multi-process lambdas...
    def run_single_x(self, raw_dsp_file: tuple[str, str], *, force: bool) -> bool:
        return self.run_single(raw_dsp_file[0], raw_dsp_file[1], force=force)
    
    def run_all(self, raw_dir, dsp_dir, *, force: bool) -> int:
        ret: int = 0
        raw_dsp_files: list[tuple[str, str]] = []
        for raw_file in glob(raw_dir+"/**/*.lh5", recursive=True):
            dsp_file = self.get_dsp_filename(raw_file=raw_file, raw_dir=raw_dir, dsp_dir=dsp_dir)
            if not os.path.exists(dirname := os.path.dirname(dsp_file)):
                os.makedirs(dirname, 0o777)
            raw_dsp_files.append((raw_file, dsp_file))

        with ProcessPoolExecutor(10) as executor:
            run_me = partial(self.run_single_x, force=force)
            for flag in executor.map(run_me, raw_dsp_files):
            #flag = self.run_single(raw_file, dsp_file, force=force)
                if flag:
                    ret += 1
        return ret 

    @staticmethod
    def get_dsp_filename(*, raw_file: str, raw_dir: str, dsp_dir: str) -> str:
        ret = raw_file
        ret = ret.replace(raw_dir, dsp_dir)
        ret = ret.replace("tier_raw", "tier_dsp")
        return ret

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            prog='run_dsp',
            description='Run dspeed raw-psd for L200 data (without snakemake)')
    parser.add_argument('--rawdir', help='The raw directory (where the raw data is stored (lh5))', required=True)
    parser.add_argument('--dspdir', help='The dsp directory', required=True)
    parser.add_argument('--config', help='DSP config file', required=True)
    #parser.add_argument('--force', action='store_true', help='Overwrite potentially existing raw data')

    args = parser.parse_args()
    if not os.path.isfile(args.config):
        raise RuntimeError(f"No file: {args.config}")
    t = Task(args.config)
    num = t.run_all(args.rawdir, args.dspdir, force=True)    
    print(f"Finished processing {num} files.")
    