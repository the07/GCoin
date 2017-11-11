import time
import hashlib
import json

class Block(object):
    """ A block holds the list of transactions. """

    transactions = []

    def __init__(self, index, transactions, previous_hash, timestamp, nonce=0):
        """
        :param index : index # of block
        :type index : integer
        :param transactions : list of transactions
        :type transactions : list of transactions object
        :param previous_hash : previous block hash
        :type previous_hash : string
        :param timestamp : timestamp of when the block is mined
        :type timestamp : integer
        :param nonce : nonce
        :type nonce : int
        """
        self._index = index
        self._transactions = transactions
        self._previous_hash = previous_hash
        self._nonce = nonce
        self._timestamp = timestamp if timestamp is not None else int(time.time())
        self._current_hash = self._calculate_block_hash()

    @property
    def index(self):
        return self._index

    @property
    def transactions(self):
        return self._transactions

    @property
    def previous_hash(self):
        return self._previous_hash

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def current_hash(self):
        return self._current_hash

    @property
    def nonce(self):
        return self._nonce

    @nonce.setter
    def nonce(self, value):
        self._nonce = value
        self._current_hash = self._calculate_block_hash()

    def _calculate_block_hash(self):
        """
        :return : sha256 hash
        :rtype : string
        """
        data = {
            "index" : self._index,
            "previous_hash" : self._previous_hash,
            "timestamp" : self._timestamp,
            "transactions" : [t.tx_hash for t in self._transactions],
            "nonce" : self._nonce
        }

        hash_object = hashlib.sha256(json.dumps(data).encode('utf-8'))
        return hash_object.hexdigest()

    def to_json(self):
        return json.dumps(self, default=lambda o: {key.lstrip('_'): value for key, value in o.__dict__.items()}, sort_keys=True)

    def __repr__(self):
        return "<GCoin Block {}>".format(self._index)

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self == other
