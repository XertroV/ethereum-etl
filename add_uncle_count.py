import csv
import json
import logging
import os
import shutil
from typing import Union
import multiprocessing as mp

import click
from more_itertools import chunked

from ethereumetl.json_rpc_requests import generate_get_block_uncle_count_by_number_json_rpc
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy
from ethereumetl.utils import hex_to_dec, rpc_response_batch_to_results


logging.basicConfig(level=logging.INFO)


def run_file(file_ix: int, file: str, n_files: int, batch_size: int, provider_uri: str):
    batch_web3_provider=ThreadLocalProxy(lambda: get_provider_from_uri(provider_uri, batch=True))
    _lp = f"[F {file_ix+1} / {n_files}]"  # log prefix
    info = lambda msg: logging.info(f"{_lp} {msg}")

    with open(file, 'r') as f:
        reader = csv.DictReader(f.readlines())
    if reader.fieldnames is None:
        raise Exception(f"reader.fieldnames is None")
    fns = list(reader.fieldnames)

    if 'uncle_count' in fns:
        info(f"!! Skipping {file} since it already has the uncle_count column")
        return

    rows = list(r for r in reader)
    blocks = [int(r['number']) for r in rows]
    batches = list(chunked(blocks, batch_size))
    n_batches = len(batches)
    uncle_counts = []

    info(f"Processing {file}... Blocks {min(blocks)} to {max(blocks)}")

    for i,batch in enumerate(batches):
        uncles_rpc = list(generate_get_block_uncle_count_by_number_json_rpc(batch))
        response = batch_web3_provider.make_batch_request(json.dumps(uncles_rpc))
        results = list(hex_to_dec(uc) for uc in rpc_response_batch_to_results(response))
        uncle_counts.extend(results)
        info(f"RPC batches done: {i+1} of {n_batches}")

    for (r, uncle_count) in zip(rows, uncle_counts):
        r['uncle_count'] = uncle_count
    fns.append('uncle_count')

    tmp_file = f"{file}.tmp"
    with open(tmp_file, 'w') as f:
        writer = csv.DictWriter(f, fieldnames=fns)
        writer.writeheader()
        writer.writerows(rows)

    shutil.move(tmp_file, file)

    info(f"Completed update of {file}")


@click.command()
@click.argument(
    'files', nargs=-1, type=click.Path(exists=True),
    # help='csv files to add uncle_count col to'
    )
@click.option('-p', '--provider-uri', default='https://mainnet.infura.io', show_default=True, type=str,
              help='The URI of the web3 provider e.g. '
                   'file://$HOME/Library/Ethereum/geth.ipc or https://mainnet.infura.io')
@click.option('-b', '--batch-size', default=1000, show_default=True, type=int, help='The number of blocks to export at a time.')
@click.option('-w', '--max-workers', default=5, show_default=True, type=int, help='The maximum number of workers.')
def run(files, provider_uri, batch_size, max_workers):
    n_files = len(files)

    if n_files == 0:
        logging.info(f"Nothing to do... Exiting.")
        return

    logging.info(f"Processing {n_files} files. Will update with uncle_count column for each block.")

    pool = mp.Pool(processes=max_workers)
    res = pool.starmap_async(run_file, [(ix, f, n_files, batch_size, provider_uri) for ix,f in enumerate(files)])
    res.get()
    pool.close()
    pool.join()

    logging.info(f"Completed all files. Done.")

run()
