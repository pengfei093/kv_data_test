from tikv_client import RawClient

client = RawClient.connect("10.33.4.213:2379")
# client.delete(b"k1")
# client.delete(b"k2")
# client.delete(b"k3")
# client.delete(b"k4")
# client.delete(b"k5")
# client = TransactionClient.connect("10.33.4.213:2379")
client.put(b"k5", b"Raw KV")
client.put(b"k1", b"Hello1")
client.put(b"k2", b",")
client.put(b"k3", b"World")
client.put(b"k4", b"!")

a1 = '12'
a2 = '23'
l = ['12'.encode('utf-8'), '23'.encode('utf-8')]
# client.put(b"k6", a1)
print(client.get(b"k1"))

print(client.batch_get([b"k1", b"k3"]))
# client.delete(b"k1")
print(client.scan(b"k1", end=b"k5", limit=10, include_start=True, include_end=True))
