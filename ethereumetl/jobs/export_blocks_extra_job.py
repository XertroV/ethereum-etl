# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import json
import logging

from ethereumetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from ethereumetl.jobs.export_receipts_job import ExportReceiptsJob
from ethereumetl.json_rpc_requests import generate_get_block_by_number_json_rpc, generate_get_block_uncle_count_by_number_json_rpc, generate_get_receipt_json_rpc
from ethereumetl.mappers.block_extra_mapper import EthBlockMapper
from ethereumetl.mappers.transaction_mapper import EthTransactionMapper
from ethereumetl.utils import hex_to_dec, rpc_response_batch_to_results, validate_range


# BROKEN_TXS = {
#     "0x7ac4949d21b5679af6a2041f470e2ed004431efefabc823ddd64f457cb65897a",
#     "0xa5465e01d820a8f2dcbe55e5fa5c36986cdc864a36bf9316c23f4762236f3941",
#     "0x58a1524a8d872504b6900351e49c5ee6fe33272884ac881ba54f9190565a9f84",
#     "0xd1f022ffa5b75ef96d0fb7379043c2d0b91bda29a48fdde925bb135ba49a1323",
#     "0xc12bd04fba091eed3161975008b69813a719f1248b0b9c74dce6a99c457a6233",
#     "0xa2d5ccc614570c8006b8615f4bdc709cea35ebf4f5738e842e701ff901ac1c5c",
#     "0xccdcc97f3139c8ed54b0c361b211dc1fc4d52bcabf715ea9a4bedd30ef0d4dbd",
#     "0xf37510b264972109a17a1a70289bd4bb097bfce60dd04dd1fa672d94b0bd1341",
#     "0x31a9cc433a2b8bd90c00ce048d90fea21406de750217a0788e22053606ecb670",
#     "0x93fb3bd9147e214b877ecacb1d5b5f93e8e8e2864d83aafe6ca133ac6816fb0a",
#     "0x846a51a0f7ee288474f60ca046b3ffbc86fbd93ce2bd9def14c3fb9e9e1ffb12",
#     "0x1e5eef53d170ce48bf418da8e46760b258b9fd1248d5439bb4d43ebd6e451646",
#     "0x46599ad372fc26e03bac8cc3601818e30b1aa2d1232e04ed24740846b37d0d99",
#     "0xc8f760283ae9c8bca685bdab0162236bc5473fb50f7202b799304792afdcb6c1",
#     "0xef39dac21b863f63f70d992dd18c449c9429679470954a15538ff29410a6711b",
#     "0x4d46501c20a8e37a174b6f58704d0d404cc4ac5fc2ca0a329643d785433a20ee",
#     "0xb6dcf129d147ab8a646dde2cbe8bd039d6687a362264429d84ea2025cab94474",
#     "0x6e7fd5d96916067c964b79f4d23a1a64b09c21ff5118f295534a6553e12fad6e",
#     "0xe3d541c1a6477bd1ba58c762b6f409bd9d0f981facdb12f5f35f1bb7962de9ea",
#     "0x70a0f7ac05ec3102ff7fe3161446b1b69e5fc737a69a2272eaff37135e695edc",
#     "0x17d6b93fb01419247ae22bba0b8caca97ec20cfcbafb4d499881c59fd18480d5",
#     "0x7c5789a050b85d6fcd131d1f7f85f3ede17c73d4f8095865fa3e97fb726ba513",
# }
BROKEN_TXS = set()

# BROKEN_BLOCKS = {
#     165804,
#     165805,
#     187794,
#     187795,
# }


# Exports blocks and transactions
class ExportBlocksExtraJob(BaseJob):
    def __init__(
            self,
            start_block,
            end_block,
            batch_size,
            batch_web3_provider,
            max_workers,
            item_exporter,
            export_blocks=True,
            export_transactions=True):
        validate_range(start_block, end_block)
        self.start_block = start_block
        self.end_block = end_block

        self.batch_web3_provider = batch_web3_provider

        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers)
        self.item_exporter = item_exporter

        self.export_blocks = export_blocks
        self.export_transactions = export_transactions
        if not self.export_blocks and not self.export_transactions:
            raise ValueError('At least one of export_blocks or export_transactions must be True')

        self.block_mapper = EthBlockMapper()
        self.transaction_mapper = EthTransactionMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(
            range(self.start_block, self.end_block + 1),
            self._export_batch,
            total_items=self.end_block - self.start_block + 1
        )

    def _export_batch(self, block_number_batch):
        # logging.warning(f"{block_number_batch.__class__} {block_number_batch}")
        # block_number_batch = list(n for n in block_number_batch if not (164_000 <= n < 166_000 or n in BROKEN_BLOCKS))
        if len(block_number_batch) == 0:
            return
        blocks_rpc = list(generate_get_block_by_number_json_rpc(block_number_batch, self.export_transactions))
        # print(blocks_rpc[:2])
        response = self.batch_web3_provider.make_batch_request(json.dumps(blocks_rpc))
        # print(response)
        results = list(rpc_response_batch_to_results(response))
        # print(results[:2])
        blocks = [self.block_mapper.json_dict_to_block(result) for result in results]

        # handle transaction fees
        for block in blocks:
            tx_hashes = list(tx.hash for tx in block.transactions if tx.hash not in BROKEN_TXS)
            # if we have no txids then shortcut the rest
            if len(tx_hashes) == 0:
                block.tx_fees = 0
                continue
            txr_requests = list(generate_get_receipt_json_rpc(tx_hashes))

            response = self.batch_web3_provider.make_batch_request(json.dumps(txr_requests))
            results = rpc_response_batch_to_results(response, request=txr_requests, block=block)
            # raise Exception(str(list(results))[:2000])
            for i,res in enumerate(results):
                if res:
                    block.transactions[i].gas_used = hex_to_dec(res.get('gasUsed'))
                # logging.warn(f"{block.transactions[i].gas_used} == {res.get('gasUsed')} ? (yes)")
            # logging.info(f"{block.transactions[-1].gas_used}")
            block.tx_fees = sum(tx.gas_price * tx.gas_used for tx in block.transactions)
            # logging.info(f"{block.number} >> {block.tx_fees}")

        # update with uncle count
        uncles_rpc = list(generate_get_block_uncle_count_by_number_json_rpc(block_number_batch))
        response = self.batch_web3_provider.make_batch_request(json.dumps(uncles_rpc))
        results = list(rpc_response_batch_to_results(response))
        for (block, uncle_count) in zip(blocks, results):
            block.uncle_count = hex_to_dec(uncle_count)

        for block in blocks:
            self._export_block(block)

    def _export_block(self, block):
        if self.export_blocks:
            self.item_exporter.export_item(self.block_mapper.block_to_dict(block))
        # if self.export_transactions:
        #     for tx in block.transactions:
        #         self.item_exporter.export_item(self.transaction_mapper.transaction_to_dict(tx))

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
