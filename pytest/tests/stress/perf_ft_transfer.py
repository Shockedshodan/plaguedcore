#!/usr/bin/env python3

"""
This is a benchmark in which a network with a single fungible_token contract is
deployed, and then a variable number of users (see `N_ACCOUNTS`) send each other
them fungible tokens.

At the time this benchmark is written, the intent is to observe the node metrics
and traces for the block duration, potentially adding any additional
instrumentation as needed.

To run:

```
env NEAR_ROOT=../target/release/ \
    python3 tests/stress/perf_ft_transfer.py \
      --fungible-token-wasm=$HOME/FT/res/fungible_token.wasm
```
"""

import argparse
import sys
import os
import time
import pathlib
import base58
import itertools
import requests
import random
import logging
import json
import multiprocessing
import multiprocessing.queues
import ctypes
import ed25519

sys.path.append(str(pathlib.Path(__file__).resolve().parents[2] / 'lib'))

import cluster
import utils
import account
import transaction
import key
import account
import mocknet_helpers
from configured_logger import new_logger

DEFAULT_TRANSACTION_TTL_SECONDS = 10
GAS_PER_BLOCK = 10E14
TRANSACTIONS_PER_BLOCK = 121
MAX_INFLIGHT_TRANSACTIONS = 5000
SEED = random.uniform(0, 0xFFFFFFFF)
logger = new_logger(level = logging.INFO)
ACCOUNTS = []
CONTRACT_ACCOUNT = None

class Transaction:
    """
    A transaction future.
    """
    def __init__(self):
        # Number of times we are going to check this transaction for completion before retrying
        # submission
        self.ttl = 0
        self.expiration = 0
        # The transaction id hash
        #
        # str if the transaction has been submitted and may eventually conclude.
        self.transaction_id = None
        # The transaction caller (used for checking the transaction status.
        #
        # `str` if the transaction has been submitted and may eventually conclude.
        self.caller = None
        # The outcome of a successful transaction execution
        self.outcome = None

    def poll(self, node, block_hash):
        """
        Returns True if transaction has completed.
        """
        if self.is_complete():
            return True
        # Send the transaction if the previous expired or we didn't send one in the first place.
        if self.transaction_id is None or self.ttl <= 0:
            if self.transaction_id is not None:
                logger.debug(f"transaction {self.transaction_id} expired, submitting a new one!")
            (self.transaction_id, self.caller) = self.send(node, block_hash)
            self.expiration = time.time() + DEFAULT_TRANSACTION_TTL_SECONDS
            self.ttl = DEFAULT_TRANSACTION_TTL_SECONDS
            return False # almost guaranteed to not produce any results right now.
        caller = ACCOUNTS[self.caller]
        logger.debug(f"checking {self.transaction_id} from {caller.key.account_id}")
        try:
            tx_result = node.json_rpc('tx', [self.transaction_id, caller.key.account_id])
            if self.is_success(tx_result):
                self.outcome = tx_result
                return True
        except requests.exceptions.ReadTimeout:
            pass
        return False

    def send(self, block_hash):
        return (self.transaction_id, self.caller)

    def is_complete(self):
        return self.outcome is not None

    def is_success(self, tx_result):
        success = 'error' not in tx_result
        if not success:
            logger.debug(f"transaction {self.transaction_id} for {self.caller} is not successful: {tx_result}")
        # only set TTL if we managed to check for success or failure...
        self.ttl = self.expiration - time.time()
        return success


class DeployFT(Transaction):
    def __init__(self, account, contract):
        super().__init__()
        self.account = account
        self.contract = contract

    def send(self, node, block_hash):
        account = ACCOUNTS[self.account]
        logger.warning(f"deploying FT to {account.key.account_id}")
        wasm_binary = utils.load_binary_file(self.contract)
        tx = transaction.sign_deploy_contract_tx(
            account.key,
            wasm_binary,
            account.use_nonce(),
            block_hash
        )
        result = node.send_tx(tx)
        return (result["result"], self.account)


class TransferFT(Transaction):
    def __init__(self, ft, sender, recipient, how_much = 1):
        super().__init__()
        self.ft = ft
        self.sender = sender
        self.recipient = recipient
        self.how_much = how_much

    def send(self, node, block_hash):
        (ft, sender, recipient) = ACCOUNTS[self.ft], ACCOUNTS[self.sender], ACCOUNTS[self.recipient]
        logger.debug(f"sending {self.how_much} FT from {sender.key.account_id} to {recipient.key.account_id}")
        args = {
            "receiver_id": recipient.key.account_id,
            "amount": str(int(self.how_much)),
        }
        tx = transaction.sign_function_call_tx(
            sender.key,
            ft.key.account_id,
            "ft_transfer",
            json.dumps(args).encode('utf-8'),
            # About enough gas per call to fit N such transactions into an average block.
            int(GAS_PER_BLOCK // TRANSACTIONS_PER_BLOCK),
            # Gotta deposit some NEAR for storage?
            1,
            sender.use_nonce(),
            block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.sender)


class TransferNear(Transaction):
    def __init__(self, sender, recipient_id, how_much = 2.0):
        super().__init__()
        self.recipient_id = recipient_id
        self.sender = sender
        self.how_much = how_much

    def send(self, node, block_hash):
        sender = ACCOUNTS[self.sender]
        logger.debug(f"sending {self.how_much} NEAR from {sender.key.account_id} to {self.recipient_id}")
        tx = transaction.sign_payment_tx(
            sender.key,
            self.recipient_id,
            int(self.how_much * 1E24),
            sender.use_nonce(),
            block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.sender)


class InitFT(Transaction):
    def __init__(self, contract):
        super().__init__()
        self.contract = contract

    def send(self, node, block_hash):
        contract = ACCOUNTS[self.contract]
        args = json.dumps({
            "owner_id": contract.key.account_id,
            "total_supply": str(10**33)
        })
        tx = transaction.sign_function_call_tx(
            contract.key,
            contract.key.account_id,
            "new_default_meta",
            args.encode('utf-8'),
            int(3E14),
            0,
            contract.use_nonce(),
            block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.contract)


class InitFTAccount(Transaction):
    def __init__(self, contract, account):
        super().__init__()
        self.contract = contract
        self.account = account

    def send(self, node, block_hash):
        contract, account = ACCOUNTS[self.contract], ACCOUNTS[self.account]
        args = json.dumps({"account_id": account.key.account_id})
        tx = transaction.sign_function_call_tx(
            contract.key,
            contract.key.account_id,
            "storage_deposit",
            args.encode('utf-8'),
            int(3E14),
            int(1E23),
            contract.use_nonce(),
            block_hash)
        result = node.send_tx(tx)
        return (result["result"], self.contract)


class TxQueue(multiprocessing.queues.Queue):
    def __init__(self, size, *args, **kwargs):
        super().__init__(size, ctx=multiprocessing.get_context(), *args, **kwargs)
        self.pending = multiprocessing.Value(ctypes.c_ulong, 0)

    def add(self, tx):
        with self.pending.get_lock():
            self.pending.value += 1
        self.put(tx)

    def complete(self):
        with self.pending.get_lock():
            self.pending.value -= 1


class Account:
    def __init__(self, key):
        self.key = key
        self.nonce = multiprocessing.Value(ctypes.c_ulong, 0)

    def refresh_nonce(self, node):
        with self.nonce.get_lock():
            self.nonce.value = mocknet_helpers.get_nonce_for_key(
                self.key,
                addr=node.rpc_addr()[0],
                port=node.rpc_addr()[1],
            )

    def use_nonce(self):
        with self.nonce.get_lock():
            new_nonce = self.nonce.value + 1
            self.nonce.value = new_nonce
            return new_nonce


def transaction_executor(nodes, tx_queue):
    block_hash = base58.b58decode(nodes[0].get_latest_block().hash)
    last_block_hash_update = time.time()
    while True:
        # TODO: pick RPC node randomly.
        # neard can handle somewhat stale block hashes, but not too stale.
        # update the block hash if it is been a while since we did so.
        now = time.time()
        if now - last_block_hash_update >= 5:
            block_hash = base58.b58decode(nodes[0].get_latest_block().hash)
            last_block_hash_update = now

        tx = tx_queue.get()
        if not tx.poll(nodes[0], block_hash):
            # Gotta make sure whoever is pushing to the queue is not filling the queue up fully.
            tx_queue.put(tx)
            if tx.ttl != DEFAULT_TRANSACTION_TTL_SECONDS:
                time.sleep(0.1) # don't spam RPC too hard...
        else:
            tx_queue.complete()

def main():
    parser = argparse.ArgumentParser(description='FT transfer benchmark.')
    parser.add_argument('--fungible-token-wasm', required=True,
        help='Path to the compiled Fungible Token contract')
    parser.add_argument('--setup-cluster', default=False,
        help='Whether to start a dedicated cluster instead of connecting to an existing local node',
        action='store_true')
    parser.add_argument('--contract-key', default=None,
        help='Account to deploy contract to and use as source of NEAR for account creation')
    parser.add_argument('--accounts', default=1000, help='Number of accounts to use')
    parser.add_argument('--no-account-topup', default=False,
        action='store_true', help='Fill accounts with additional NEAR prior to testing')
    parser.add_argument('--shards', default=10, help='number of shards')
    args = parser.parse_args()

    logger.warning(f"SEED is {SEED}")
    rng = random.Random(SEED)

    if args.setup_cluster:
        config = cluster.load_config()
        nodes = cluster.start_cluster(2, 0, args.shards, config, [["epoch_length", 100]], {
            shard: { "tracked_shards": list(range(args.shards)) }
            for shard in range(args.shards + 1)
        })
        if args.contract_key is None:
            signer_key = nodes[0].signer_key
        else:
            signer_key = key.Key.from_json_file(args.contract_key)

    else:
        nodes = [
            cluster.RpcNode("127.0.0.1", 3030),
        ]
        # The `nearup` localnet setup stores the keys in this directory.
        if args.contract_key is None:
            key_path = (pathlib.Path.home() / ".near/localnet/node0/shard0_key.json").resolve()
        else:
            key_path = args.contract_key
        signer_key = key.Key.from_json_file(key_path)

    ACCOUNTS.append(Account(signer_key))
    ACCOUNTS[0].refresh_nonce(nodes[0])
    contract_account_idx = 0
    for i in range(int(args.accounts)):
        keys = ed25519.create_keypair(entropy=rng.randbytes)
        account_id = keys[1].to_bytes().hex()
        sk = 'ed25519:' + base58.b58encode(keys[0].to_bytes()).decode('ascii')
        pk = 'ed25519:' + base58.b58encode(keys[1].to_bytes()).decode('ascii')
        ACCOUNTS.append(Account(key.Key(account_id, pk, sk)))


    queue_size = min(10240, int(args.accounts) + 50)
    tx_queue = TxQueue(queue_size)
    multiprocessing.Process(target=transaction_executor, args=(nodes, tx_queue,), daemon=True).start()
    multiprocessing.Process(target=transaction_executor, args=(nodes, tx_queue,), daemon=True).start()
    multiprocessing.Process(target=transaction_executor, args=(nodes, tx_queue,), daemon=True).start()
    multiprocessing.Process(target=transaction_executor, args=(nodes, tx_queue,), daemon=True).start()

    tx_queue.add(DeployFT(contract_account_idx, args.fungible_token_wasm))
    wait_empty(tx_queue, "deployment")
    tx_queue.add(InitFT(contract_account_idx))
    wait_empty(tx_queue, "contract initialization")

    if not args.no_account_topup:
        for test_account in ACCOUNTS[contract_account_idx:]:
            tx_queue.add(TransferNear(contract_account_idx, test_account.key.account_id, 2.0))
        wait_empty(tx_queue, "account creation and top-up")

    # Refresh nonces for all real accounts.
    for account in ACCOUNTS[contract_account_idx:]:
        account.refresh_nonce(nodes[0])

    for test_account_idx in range(contract_account_idx, len(ACCOUNTS)):
        tx_queue.add(InitFTAccount(contract_account_idx, test_account_idx))
    wait_empty(tx_queue, "init accounts with the FT contract")

    for test_account_idx in range(contract_account_idx, len(ACCOUNTS)):
        tx_queue.add(TransferFT(
            contract_account_idx, contract_account_idx, test_account_idx, how_much=1E8
        ))
    wait_empty(tx_queue, "distribution of initial FT")

    transfers = 0
    while True:
        sender_idx, receiver_idx = rng.sample(range(contract_account_idx, len(ACCOUNTS)), k=2)
        tx_queue.add(TransferFT(contract_account_idx, sender_idx, receiver_idx, how_much=1))
        transfers += 1
        if transfers % 10000 == 0:
            logger.info(f"{transfers} so far ({tx_queue.pending.value} pending)")
        while tx_queue.pending.value >= MAX_INFLIGHT_TRANSACTIONS:
            time.sleep(0.25)

def wait_empty(queue, why):
    with queue.pending.get_lock():
        remaining = queue.pending.value
    while remaining != 0:
        logger.info(f"waiting for {why} ({remaining} remain)")
        time.sleep(0.5)
        with queue.pending.get_lock():
            remaining = queue.pending.value
    logger.info(f"wait for {why} completed!")

if __name__ == "__main__":
    main()
