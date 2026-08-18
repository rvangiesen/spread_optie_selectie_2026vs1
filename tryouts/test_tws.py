from ib_client import IBClient
client = IBClient()
success, msg = client.connect('127.0.0.1', 7497, 999)
print(f"Connection success: {success}, msg: {msg}")
client.disconnect()
