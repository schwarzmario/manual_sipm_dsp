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

all_sipms = ["S061", "S055", "S017", "S083", "S073", "S071", "S070", "S067", "S068", "S029", 
             "S042", "S041", "S023", "S030", "S031", "S002", "S003", "S032", "S036", "S094", 
             "S098", "S008", "S058", "S057", "S095", "S099", "S065", "S087", "S082", "S046", 
             "S047", "S011", "S012", "S020", "S080", "S026", "S025", "S015", "S043", "S040", 
             "S048", "S049", "S053", "S052", "S050", "S051", "S085", "S086", "S037", "S007"]

class Task:
    def __init__(self, config_file):
        self.config_file = config_file

    def run_single(self, raw_file: str, dsp_file: str, *, use_sipms:list[str]|None = None, force: bool) -> bool:
        if use_sipms is None:
            use_sipms = all_sipms
        if not force and os.path.exists(dsp_file):
            return False
        print("Calling build_dsp for", raw_file, "->", dsp_file, "with config", self.config_file)
        build_dsp(
            raw_file,
            dsp_file,
            dsp_config=self.config_file,
            lh5_tables=use_sipms,
            write_mode="r"  # used to overwrite an existing file
        )
        return True
    
    #stupid python does not want to multi-process lambdas...
    def run_single_x(self, raw_dsp_file: tuple[str, str], *, 
                     use_sipms:list[str]|None = None, force: bool) -> bool:
        return self.run_single(raw_dsp_file[0], raw_dsp_file[1], use_sipms=use_sipms, force=force)
    
    def run_all(self, raw_dir, dsp_dir, *, use_sipms:list[str]|None = None, force: bool) -> int:
        ret: int = 0
        raw_dsp_files: list[tuple[str, str]] = []
        for raw_file in glob(raw_dir+"/**/*.lh5", recursive=True):
            dsp_file = self.get_dsp_filename(raw_file=raw_file, raw_dir=raw_dir, dsp_dir=dsp_dir)
            if not os.path.exists(dirname := os.path.dirname(dsp_file)):
                os.makedirs(dirname, 0o777)
            raw_dsp_files.append((raw_file, dsp_file))

        with ProcessPoolExecutor(10) as executor:
            run_me = partial(self.run_single_x, use_sipms=use_sipms, force=force)
            for flag in executor.map(run_me, raw_dsp_files):
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
    parser.add_argument('--sipms', help='which SiPMs to process. Defaults to all.', nargs='+')
    #parser.add_argument('--force', action='store_true', help='Overwrite potentially existing raw data')

    args = parser.parse_args()
    if not os.path.isfile(args.config):
        raise RuntimeError(f"No file: {args.config}")
    t = Task(args.config)
    num = t.run_all(args.rawdir, args.dspdir, use_sipms=args.sipms, force=True)
    print(f"Finished processing {num} files.")
    