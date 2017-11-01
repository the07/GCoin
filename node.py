import grequests
import requests

from blockchain import *
from klein import Klein
from transaction import *

FULL_NODE_PORT = "30013"
NODES_URL = "http://{}:{}/nodes"
TRANSACTIONS_URL = "http://{}:{}/transactions"
BLOCK_URL = "http://{}:{}/block/{}"
BLOCKS_RANGE_URL = "http://{}:{}/blocks/{}/{}"
BLOCKS_URL = "http://{}:{}/blocks"
TRANSACTION_HISTORY_URL = "http://{}:{}/address/{}/transactions"
BALANCE_URL = "http://{}:{}/address/{}/balance"

class NodeMixin(object):
    # TODO : store the nodes in an external config file.
    full_nodes = {"127.0.0.1"}

    def request_nodes(self, node, port):
        url = NODES_URL.format(node, port)
        try:
            response = requests.get(url)
            if response.status_code == 200:
                all_nodes = response.json()
                return all_nodes
        except requests.exceptions.RequestException as re:
            pass
        return None

    def request_nodes_from_all(self):
        full_nodes = self.full_nodes.copy()
        bad_nodes = set()

        for node in full_nodes:
            all_nodes = self.request_nodes(node, FULL_NODE_PORT)
            if all_nodes is not None:
                full_nodes = full_nodes.union(all_nodes["full_nodes"])
            else:
                bad_nodes.add(node)
        self.full_nodes = full_nodes

        for node in bad_nodes:
            self.remove_node(node)
        return

    def remove_node(self, node):
        # TODO : Implement removal of nodes
        pass

    def broadcast_transaction(self, transaction):
        self.request_nodes_from_all()
        bad_nodes = set()
        data = {
            "transaction": transaction.to_json()
        }

        for node in self.full_nodes:
            url = TRANSACTIONS_URL.format(node, FULL_NODE_PORT)
            try:
                response = requests.post(url, json=data)
            except requests.exceptions.RequestException as re:
                bad_nodes.add(node)
        for node in bad_nodes:
            self.remove_node(node)
        bad_nodes.clear()
        return
        # TODO : Change implementation to grequests and return list of responses

class FullNode(NodeMixin):
    NODE_TYPE = "full"
    blockchain = None
    app = Klein()

    def __init__(self, host, reward_address, block_path=None):
        self.host = host
        self.request_nodes_from_all()
        self.reward_address = reward_address
        self.broadcast_node(host)
        self.full_nodes.add(host)
        if block_path is None:
            self.blockchain = Blockchain()
        else:
            self.load_blockchain(block_path)

        thread = threading.Thread(target=self.mine, args=())
        thread.daemon = True
        thread.start()
        print ("\nFull Node Server started...\n\n")
        self.app.run(host, FULL_NODE_PORT)

    def request_block(self, node, port, index="latest"):
        url = BLOCKS_URL.format(node, port, index)
        try:
            response = requests.get(url)
            if response.status_code == 200:
                block_dict = json.loads(response.json())
                block = Block(
                    block_dict['index'],
                    block_dict['transactions'],
                    block_dict['previous_hash'],
                    block_dict['timestamp'],
                    block_dict['nonce']
                )
                if block.current_hash != block_dict['current_hash']:
                    raise InvalidHash(block.index, "Block Hash Mismatch: {} {}".format(block_dict['current_hash'], block.current_hash))
                return block
        except requests.exceptions.RequestException as re:
            pass
        return None

    def request_block_from_all(self, index="latest"):
        blocks = []

        full_nodes = self.full_nodes.copy()
        bad_nodes = set()

        for node in full_nodes:
            block = self.request_block(node, FULL_NODE_PORT, index)
            if block is not None:
                blocks.append(block)
            else:
                bad_nodes.add(node)

        for node in bad_nodes:
            self.remove_node(node)
        return blocks

    def request_blocks_range(self, node, port, start_index, stop_index):
        url = BLOCKS_RANGE_URL.format(node, port, start_index, stop_index)
        blocks = []

        try:
            response = requests.get(url)
            if response.status_code == 200:
                blocks_dict = json.loads(response.json())
                for block_dict in blocks_dict:
                    block = Block(
                        block_dict['index'],
                        block_dict['transactions'],
                        block_dict['previous_hash'],
                        block_dict['timestamp'],
                        block_dict['nonce']
                    )
                    if block.current_hash != block_dict['current_hash']:
                        raise InvalidHash(block.index, "Block Hash Mismatch: {}".format(block_dict['current_hash']))
                    blocks.append(block)
                return blocks
        except requests.exceptions.RequestException as re:
            pass
        return None

    def request_blockchain(self, node, port):
        url = BLOCKS_URL.format(node, port)
        blocks = []
        try:
            response = requests.get(url)
            if response.status_code == 200:
                blocks_dict = json.loads(resonse.json())
                for block_dict in blocks_dict:
                    block = Block(
                        block_dict['index'],
                        block_dict['transactions'],
                        block_dict['previous_hash'],
                        block_dict['timestamp'],
                        block_dict['nonce'],
                    )
                    if block.current_hash != block_dict['current_hash']:
                        raise InvalidHash(block.index, "Block Hash Mismatch: {}".format(block_dict['current_hash']))
                    blocks.append(block)
                return blocks
        except requests.exceptions.RequestException as re:
            pass
        return None

    def mine(self):
        print ("\n\n Mining started...\n\n")
        while True:
            latest_block = self.blockchain.get_latest_block()
            latest_hash = latest_block.current_hash
            latest_index = latest_block.index

            block = self.blockchain.mine_block(self, reward_address)
            if not block:
                continue
            statuses = self.broadcast_block(block)
            if statuses['expirations'] > statuses['confirmations'] or statuses['invalidations'] > statuses['confirmations']:
                self.synchronize()
                new_latest_block = self.blockchain.get_latest_block()
                if latest_hash != new_latest_block.current_hash or latest_index != new_latest_block.index:
                    # Latest block changed after the sync, do not add the block
                    self.blockchain.recycle_transactions(block)
                    continue
            self.blockchain.add_block(block)

    def broadcast_block(self, block):
        # TODO : convert to grequests and concurrently gather a list of responses
        statuses = {
            "confirmations": 0,
            "invalidations": 0,
            "expirations": 0
        }

        self.request_nodes_from_all()
        bad_nodes = set()
        data = {
            "block": block.to_json(),
            "host": self.host
        }

        for node in self.full_nodes:
            if node == self.host:
                continue
            url = BLOCKS_URL.format(node, FULL_NODE_PORT)
            try:
                response = requests.post(url, json=data)
                if response.status_code == 202:
                    # Confirmed and accepted by node
                    statuses["confirmations"] += 1
                elif response.status_code == 406:
                    # Invalidated and rejected by the node
                    statuses["invalidations"] += 1
                elif response.status_code == 409:
                    # expired and rejected by the node
                    statuses["expirations"] += 1
            except requests.exceptions.RequestException as re:
                bad_nodes.add(node)

        for node in bad_nodes:
            self.remove_node(node)
        bad_nodes.clear()
        return statuses

    def add_node(self, host):
        if host == self.host:
            return

        if host not in self.full_nodes:
            self.broadcast_node(host)
            self.full_nodes.add(host)

    def broadcast_node(self, host):
        self.request_nodes_from_all()
        bad_nodes = set()
        data = {
            "host": host
        }

        for node in self.full_nodes:
            if node == self.host:
                continue
            url = NODES_URL.format(node, FULL_NODE_PORT)
            try:
                requests.post(url, json=data)
            except requests.exceptions.RequestException as re:
                bad_nodes.add(node)

        for node in bad_nodes:
            self.remove_node(node)
        bad_nodes.clear()
        return

    def load_blockchain(self):
        # TODO : load blockchain from the path
        pass

    def synchronize(self):
        my_latest_block = self.blockchain.get_latest_block()
        latest_blocks = {}

        self.request_nodes_from_all()
        bad_nodes = set()
        for node in self.full_nodes:
            url = BLOCK_URL.format(node, FULL_NODE_PORT, "latest")
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    remote_latest_block = response.json()
                    if remote_latest_block["index"] <= my_latest_block.index:
                        continue
                    if latest_blocks.get(remote_latest_block["index"], None) is None:
                        latest_blocks[remote_latest_block["index"]] = {
                            remote_latest_block["current_hash"]: [node]
                        }
                        continue
                    if latest_blocks[remote_latest_block["index"]].get(remote_latest_block["current_hash"], None) is None:
                        latest_blocks[remote_latest_block["index"]][remote_latest_block["current_hash"]] = [node]
                        continue
                    latest_blocks[remote_latest_block["index"]][remote_latest_block["current_hash"]].append(node)
            except requests.exceptions.RequestException as re:
                bad_nodes.add(node)
            if len(latest_blocks) > 0:
                for latest_block in sorted(latest_blocks.items(), reverse=True):
                    index = latest_block[0]
                    current_hashes = latest_block[1]
                    success = True
                    for current_hash in current_hashes:

                        remote_host = current_hash[1][0]

                        remote_diff_blocks = self.request_blocks_range(
                            remote_host,
                            FULL_NODE_PORT,
                            my_latest_block.index + 1,
                            index
                        )
                        if remote_diff_blocks[0].previous_hash == my_latest_block.current_hash:

                            # First block in diff block fits local Chain
                            for block in remote_diff_blocks:
                                result = self.blockchain.add_block(block)
                                if not result:
                                    success = False
                                    break
                        else:
                            # First block in diff block does not fit local chain
                            for i in range(my_latest_block, 1, -1):
                                # Step back and look for the first remote block that fits the chain
                                block = self.request_block(remote_host, FULL_NODE_PORT, str(i))
                                remote_diff_blocks[0:0] = [block]
                                if block.previous_hash == self.blockchain.get_block_by_index(i-1):
                                    # Found the fork
                                    result = self.blockchain.alter_chain(remote_diff_blocks)
                                    success = result
                                    break
                            success = False
                        if success:
                            break
                    if success:
                        break
            return



    @app.route('/transactions', methods=['POST'])
    def post_transactions(self, request):
        body = json.loads(request.content.read())
        transaction = Transaction.from_json(body['transaction'])
        return json.dumps({'success': self.blockchain.push_unconfirmed_transaction(transaction)})

    @app.route('/transactions', methods=['GET'])
    def get_transactions(self, request):
        return json.dumps(self.blockchain.get_all_unconfirmed_transactions())

    @app.route('/address/<address>/balance', methods=['GET'])
    def get_balance(self, request, address):
        return json.dumps(self.blockchain.get_balance(address))

    @app.route('/address/<address>/transactions', methods=['GET'])
    def get_transaction_history(self, request, address):
        return json.dumps(self.blockchain.get_transaction_history(address))

    @app.route('/blocks', methods=['POST'])
    def post_block(self, request):
        body = json.loads(request.content.read())
        remote_block = json.loads(body['block'])
        remote_host = body['host']
        transactions = [
            Transaction.from_json(transaction_json) for transaction_json in remote_block['transactions']
        ]
        block = Block(
            remote_block['index'],
            transactions,
            remote_block['previous_hash'],
            remote_block['timestamp'],
            remote_block['nonce']
        )
        if block.current_hash != remote_block['current_hash']:
            request.setResponseCode(406)  # not acceptable
            return json.dumps({'message': 'block rejected due to invalid hash'})
        my_latest_block = self.blockchain.get_latest_block()

        if block.index > my_latest_block.index + 1:
            # new block index is greater than ours
            remote_diff_blocks = self.request_blocks_range(
                remote_host,
                FULL_NODE_PORT,
                my_latest_block.index + 1,
                remote_block['index']
            )

            if remote_diff_blocks[0].previous_hash == my_latest_block.current_hash:
                # first block in diff blocks fit local chain
                for block in remote_diff_blocks:
                    result = self.blockchain.add_block(block)
                    if not result:
                        request.setResponseCode(406)  # not acceptable
                        return json.dumps({'message': 'block {} rejected'.format(block.index)})
                request.setResponseCode(202)  # accepted
                return json.dumps({'message': 'accepted'})
            else:
                # first block in diff blocks does not fit local chain
                for i in range(my_latest_block.index, 1, -1):
                    # step backwards and look for the first remote block that fits the local chain
                    block = self.request_block(remote_host, FULL_NODE_PORT, str(i))
                    remote_diff_blocks[0:0] = [block]
                    if block.previous_hash == self.blockchain.get_block_by_index(i-1):
                        # found the fork
                        result = self.blockchain.alter_chain(remote_diff_blocks)
                        if not result:
                            request.setResponseCode(406)  # not acceptable
                            return json.dumps({'message': 'blocks rejected'})
                        request.setResponseCode(202)  # accepted
                        return json.dumps({'message': 'accepted'})
                request.setResponseCode(406)  # not acceptable
                return json.dumps({'message': 'blocks rejected'})

        elif block.index <= my_latest_block.index:
            # new block index is less than ours
            request.setResponseCode(409)  # conflict
            return json.dumps({'message': 'Block index too low.  Fetch latest chain.'})

        # correct block index. verify txs, hash
        result = self.blockchain.add_block(block)
        if not result:
            request.setResponseCode(406)  # not acceptable
            return json.dumps({'message': 'block {} rejected'.format(block.index)})
        request.setResponseCode(202)  # accepted
        return json.dumps({'message': 'accepted'})

    @app.route('/blocks', methods=['GET'])
    def get_blocks(self, request):
        return json.dumps([block.__dict__ for block in self.blockchain.get_all_blocks()])

    @app.route('/blocks/<start_block_id>/<end_block_id>', methods=['GET'])
    def get_blocks_range(self, request, start_block_id, end_block_id):
        return json.dumps([block.__dict__ for block in self.blockchain.get_blocks_range(start_block_id, end_block_id)])

    @app.route('/block/<block_id>', methods=['GET'])
    def get_block(self, request, block_id):
        if block_id == "latest":
            return json.dumps(self.blockchain.get_latest_block().__dict__)
        return json.dumps(self.blockchain.get_block_by_index(block_id).__dict__)


if __name__ == "__main__":
    pass
