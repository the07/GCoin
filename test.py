from __init__ import *

wallet = Client()
print (wallet.get_pubkey())

host = '127.0.0.1'
node1 = FullNode(host, wallet.get_pubkey())

print (wallet.get_balance())
