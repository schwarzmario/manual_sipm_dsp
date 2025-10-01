#!/usr/bin/env python3

import subprocess
import argparse
import sys
import os
from itertools import product

def main():
    parser = argparse.ArgumentParser(description='Sync files from remote directory to local using rsync.')
    parser.add_argument('--remote-host', default='legend-login1',
                        help='Remote host (default: legend-login1)')
    parser.add_argument('--remote-prefix', default=None,
                        help='Remote path prefix (if not provided, defaults to /data1/shared/l200-p13-v2.1/generated/tier for period p13, or /data2/public/prodenv/prod-blind/tmp-auto/generated/tier/ for non-raw tiers or /data2/public/prodenv/prod-blind/ref-raw/generated/tier/ for raw tier otherwise)')
    parser.add_argument('--local-prefix', default='/mnt/atlas02/projects/legend/sipm_qc/data/tier',
                        help='Local path prefix (default: /mnt/atlas02/projects/legend/sipm_qc/data/tier)')
    parser.add_argument('--tier', nargs='+', required=True,
                        help='Tier(s) (e.g., dsp or raw), space-separated for multiple')
    parser.add_argument('--type', nargs='+', required=True,
                        help='File type(s) (e.g., phy), space-separated for multiple')
    parser.add_argument('--period', nargs='+', required=True,
                        help='Period(s) (e.g., p15), space-separated for multiple')
    parser.add_argument('--run', nargs='+', required=True,
                        help='Run(s) (e.g., r004), space-separated for multiple')
    parser.add_argument('--dry-run', action='store_true',
                        help='Dry run: print paths and command without performing the sync')
    parser.add_argument('--max-files', type=int, default=None,
                        help='Maximum number of files to sync per combination, sorted by filename (0 to skip all for that combination, default: all)')

    args = parser.parse_args()

    tiers = args.tier
    types_ = args.type
    periods = args.period
    runs = args.run

    failed = False

    for tier, typ, period, runn in product(tiers, types_, periods, runs):
        if args.remote_prefix is None:
            if period == 'p13':
                remote_prefix = '/data1/shared/l200-p13-v2.1/generated/tier'
            elif tier == 'raw':
                remote_prefix = '/data2/public/prodenv/prod-blind/ref-raw/generated/tier'
            else:
                remote_prefix = '/data2/public/prodenv/prod-blind/tmp-auto/generated/tier'
        else:
            remote_prefix = args.remote_prefix

        remote_dir = f"{args.remote_host}:{remote_prefix}/{tier}/{typ}/{period}/{runn}/"
        local_path = f"{args.local_prefix}/{tier}/{typ}/{period}/{runn}"

        print(f"Processing {tier}/{typ}/{period}/{runn}: from {remote_dir} to {local_path}")

        # List files on remote
        list_cmd = ['ssh', args.remote_host, f'ls -1 "{remote_prefix}/{tier}/{typ}/{period}/{runn}/"']
        try:
            result = subprocess.run(list_cmd, capture_output=True, text=True, check=True)
            files = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        except subprocess.CalledProcessError:
            print(f"Skipping {tier}/{typ}/{period}/{runn}: directory not accessible.")
            continue

        if not files:
            print(f"Skipping {tier}/{typ}/{period}/{runn}: no files.")
            continue

        files.sort()

        if args.max_files is not None:
            files = files[:args.max_files] if args.max_files > 0 else []

        if not files:
            print(f"Skipping {tier}/{typ}/{period}/{runn}: max-files limits to 0 files.")
            continue

        print(f"Found {len(files)} files to sync for {tier}/{typ}/{period}/{runn}")

        if args.dry_run:
            if args.max_files is None:
                dry_cmd = ['rsync', '-hvPtzr', f"{remote_dir}*", local_path]
                print(f"Dry run: would sync all files from {remote_dir}* to {local_path}")
                print("Command: " + " ".join(dry_cmd))
            else:
                sources = [f"{remote_dir}{f}" for f in files]
                dry_cmd = ['rsync', '-hvPtzr'] + sources + [local_path]
                print(f"Dry run: would sync the following {len(files)} files:")
                for f in files:
                    print(f"  {f}")
                print("Command: " + " ".join(dry_cmd))
            continue

        # Actual sync
        os.makedirs(local_path, exist_ok=True)

        if args.max_files is None:
            sync_cmd = ['rsync', '-hvPtzr', f"{remote_dir}*", local_path]
        else:
            sources = [f"{remote_dir}{f}" for f in files]
            sync_cmd = ['rsync', '-hvPtzr'] + sources + [local_path]

        try:
            subprocess.run(sync_cmd, check=True)
            print(f"Sync completed successfully for {tier}/{typ}/{period}/{runn}.")
        except subprocess.CalledProcessError as e:
            print(f"Sync failed for {tier}/{typ}/{period}/{runn} with error: {e}", file=sys.stderr)
            failed = True

    if failed:
        sys.exit(1)
    else:
        print("All syncs completed successfully.")

if __name__ == '__main__':
    main()
