import hashlib
import logging
import pyelliptic
import threading
import time

from block import *
from errors import *
from transaction import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Blockchain(object):

    INITIAL_COINS_PER_BLOCK = 50
    HALVING_FREQUENCY = 1000
    MAX_TRANSACTIONS_PER_BLOCK = 10

    unconfirmed_transactions = []
    blocks = []

    def __init__(self, blocks=None):
        self.unconfirmed_transactions_lock = threading.Lock()
        self.blocks_lock = threading.Lock()
        if blocks in None:
            genesis_block = self.get_genesis_block()
            self.add_block(genesis_block)
        else:
            for block in blocks:
                self.add_block(block)

    def get_genesis_block(self):
        genesis_transaction_one = Transaction(
            "0",
            "0409eb9224f408ece7163f40a33274d99ab6b3f60e41b447dd45fcc6371f57b88d9d3583c358b1ea8aea4422d17c57de1418554d3a1cd620ca4cb296357888ea596",
            1000
        )
        genesis_transaction_two = Transaction(
            "0",
            "0466f992cd361e24e4fa0eeca9a7ddbea1d257a2053dbe16aeb36ac155679a797bf89776903290d7c93e4b5ba49968fbf8ab8a49190f3d7cafe11cc6e925e489f6",
            1000
        )

        genesis_transactions = [genesis_transaction_one, genesis_transaction_two]
        genesis_block = Block(0, genesis_transactions, 0,0,0)
        return genesis_block

    def _check_genesis_block(self, block):
        if block != self.get_genesis_block():
            raise GenesisBlockMismatch(block.index, "Genesis Block Mismatch: {}".format(block))
        return

    def _check_hash_and_hash_pattern(self, block):
        if block.current_hash[:4] != "0000":
            raise InvalidHash(block.index, "Incompatible Block Hash: {}".format(block.current_hash))
        return

    def _check_index_and_previous_hash(self, block):
        latest_block = self.get_latest_block()
        if latest_block.index != block.index - 1:
            raise ChainContinuityError(block.index, "Incompatible block index : {}".format(block.index - 1))
        if latest_block.current_hash != block.previous_hash:
            raise ChainContinuityError(block.index, "Incompatible block index : {} and hash : {}".format(block.index-1, block.previous_hash))
        return

    def _check_transactions_and_block_reward(self, block):
        payers = dict()
        for transaction in block.transaction[:-1]:
            if self.find_duplicate_transaction(transaction.tx_hash):
                raise InvalidTransactions(block.index, "Transaction not valid. Duplicate transaction detected.")
            if not transaction.verify():
                raise InvalidTransactions(block.index, "Transactions not valid. Invalid Transaction signature.")
            if transaction.source in payers:
                payers[transaction.source] += transaction.amount
            else:
                payers[transaction.source] = transaction.amount

        for key in payers:
            balance = self.get_balance(key)
            if payers[key] > balance:
                raise InvalidTransactions(block.index, "Transaction not valid. Insufficient Funds")

        # Last transaction is block reward
        reward_transaction = block.transaction[-1]
        reward_amount = self.get_reward(block.index)
        if reward_transaction.amount != reward_amount or reward_transaction.source != "0":
            raise InvalidTransactions(block.index, "Transaction not valid. Incorrect block reward.")
        return

    def validate_block(self, block):
        # verify genesis block integrity
        # TODO implement and use Merkle Tree
        try:
            # if genesis block, check if block is correct
            if block.index == 0:
                self._check_genesis_block(block)
                return True

            # Current hash of data is correct and hash satisfies pattern
            self._check_hash_and_hash_pattern(block)
            # Block index is correct and previous hash is correct
            self._check_index_and_previous_hash(block)
            # Block reward is correct based on block index and halving formula
            self._check_transactions_and_block_reward(block)
        except BlockchainException as bce:
            logger.warning("Validation Error (block id : %s): %s", bce.index, bce.message)
            return False
        return True

    def alter_chain(self, blocks):
        # TODO enforce finality through key blocks
        fork_start = blocks[0].index
        alternate_blocks = self.blocks[0:fork_start]
        alternate_blocks.extend(blocks)
        alternate_chain = Blockchain(alternate_blocks)
        if alternate_chain.get_size() > self.get_size():
            with self.blocks_lock:
                self.blocks = alternate_blocks
                return True
        return False

    def add_block(self, block):
        # TODO change this from memory to persistent
        with self.blocks_lock:
            if self.validate_block(block):
                self.blocks.append(block)
                return True
        return False

    def mine_block(self, reward_address):
        # TODO add transaction fees
        transactions = []
        latest_block = self.get_latest_block()
        new_block_id = latest_block.index + 1
        previous_hash = latest_block.current_hash

        for i in range(0, self.MAX_TRANSACTIONS_PER_BLOCK):
            unconfirmed_transactions_json = self.pop_next_uncomfirmed_transaction()
            if unconfirmed_transactions_json is None:
                break
            unconfirmed_transaction = Transaction(
                unconfirmed_transactions_json.get('source'),
                unconfirmed_transactions_json.get('destination'),
                unconfirmed_transactions_json.get('amount'),
                unconfirmed_transactions_json.get('signature')
            )

            if unconfirmed_transaction.tx_hash != unconfirmed_transactions_json.get('tx_hash'):
                continue
            if unconfirmed_transaction.tx_hash in [transaction.tx_hash for transaction in transactions]:
                continue
            if self.find_duplicate_transaction(unconfirmed_transaction.tx_hash):
                continue
            if not unconfirmed_transaction.verify():
                continue

            transactions.append(unconfirmed_transaction)

        if len(transactions) < 1:
            return None

        reward_transaction = Transaction(
            "0",
            reward_address,
            self.get_reward(new_block_id),
            "0"
        )

        transactions.append(reward_transaction)

        timestamp = int(time.time())

        i = 0
        block = Block(new_block_id, transactions, previous_hash, timestamp, i)
        while block.current_hash[:4] != "0000":
            latest_block = self.get_latest_block
            if latest_block.index >= new_block_id or latest_block.current_hash != previous_hash:
                # Next block in sequence was mined by another block
                # identify in-progress transactions that are not included in the latest_block and place them back in
                # the unconfirmed transaction pool.
                for transaction in transactions[:-1]:
                    if transaction not in latest_block.transactions:
                        self.push_unconfirmed_transaction(transaction)
                return None
            i += 1
            block.nonce = i
        return block

    def get_transaction_history(self, address):
        transactions = []
        for block in self.blocks:
            for transaction in block.transactions:
                if transaction.source == address or transaction.destination == address:
                    transactions.append(transaction)
        return transactions

    def get_balance(self, address):
        balance = 0
        for block in self.blocks:
            for transaction in block.transactions:
                if transaction.source == address:
                    balance -= transaction.amount
                if transaction.destination == address:
                    balance += transaction.amount
        return balance

    def find_duplicate_transaction(self, transaction_hash):
        for block in self.blocks:
            for transaction in block.transactions:
                if transaction.tx_hash == transaction_hash:
                    return block.index
        return False

    def recycle_transactions(self, block):
        for transaction in block.transactions[:-1]:
            if not self.find_duplicate_transaction(transaction.tx_hash):
                self.push_unconfirmed_transaction(transaction)
        return

    def validate_chain(self):
        try:
            for block in self.blocks:
                self.validate_block(block)
        except BlockchainException as bce:
            raise
        return True

    def get_reward(self, index):
        # 50 coins per block, halved every 1000 blocks
        reward = self.INITIAL_COINS_PER_BLOCK
        for i in range(1, ((index / self.HALVING_FREQUENCY) + 1)):
            reward = reward / 2
        return reward

    def get_size(self):
        return len(self.blocks)

    def get_latest_block(self):
        try:
            return self.blocks[-1]
        except IndexError:
            return None

    def get_block_by_index(self, index):
        try:
            return self.blocks[index]
        except IndexError:
            return None

    def get_all_blocks(self):
        return self.blocks

    def get_block_range(self, start_index, stop_index):
        return self.blocks[start_index:stop_index+1]

    def get_all_unconfirmed_transactions(self):
        return self.unconfirmed_transactions

    def pop_next_uncomfirmed_transaction(self):
        try:
            with self.unconfirmed_transactions_lock:
                return self.unconfirmed_transactions.pop(0)
        except IndexError:
            return None

    def push_unconfirmed_transaction(self, transaction):
        with self.unconfirmed_transactions_lock:
            self.unconfirmed_transactions.append(transaction)
            return True

    def verify_signature(self, signature, message, public_key):
        return pyelliptic.ECC(curve='secp256k1', pubkey=public_key.decode('hex')).verify(signature.decode('hex'), message)

    def generate_signable_transaction(self, from_address, to_address, amount, timestamp):
        return ":".join((from_address, to_address, amount, timestamp))

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self == other


if __name__ == "__main__":
    pass
