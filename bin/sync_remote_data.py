#!/usr/bin/env python3

import subprocess
import argparse
import sys

def main():
    parser = argparse.ArgumentParser(description='Sync files from remote directory to local using rsync.')
    parser.add_argument('--remote-host', default='legend-login1',
                        help='Remote host (default: legend-login1)')
    parser.add_argument('--remote-prefix', default='/data2/public/prodenv/prod-blind/tmp-auto/generated/',
                        help='Remote path prefix (default: /data2/public/prodenv/prod-blind/tmp-auto/generated/)')
    parser.add_argument('--local-prefix', required=True,
                        help='Local path prefix (required)')
    parser.add_argument('--tier', required=True,
                        help='Tier (e.g., dsp)')
    parser.add_argument('--type', required=True,
                        help='File type (e.g., phy)')
    parser.add_argument('--period', required=True,
                        help='Period (e.g., p15)')
    parser.add_argument('--run', required=True,
                        help='Run (e.g., r004)')

    args = parser.parse_args()

    remote_path = f"{args.remote_host}:{args.remote_prefix}{args.tier}/{args.type}/{args.period}/{args.run}/*"
    local_path = f"{args.local_prefix}{args.tier}/{args.type}/{args.period}/{args.run}"

    print(f"Syncing from {remote_path} to {local_path}")

    cmd = ['rsync', '-hvPtzr', remote_path, local_path]
    try:
        subprocess.run(cmd, check=True)
        print("Sync completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Sync failed with error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()
