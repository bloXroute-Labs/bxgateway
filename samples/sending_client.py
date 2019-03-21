import random
import socket
import struct

CLIENT_IP = '127.0.0.1'
CLIENT_PORT = 9002
MESSAGE_LEN = 1024

# Setup socket and connect
clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
clientsocket.connect((CLIENT_IP, CLIENT_PORT))

# Create and print out message
msg = bytearray(4) + bytearray(random.getrandbits(8) for _ in range(1024))
struct.pack_into('<I', msg, 0, 1024)
print("First 100 bytes of message: " + repr(msg[:100]))

# Sent message
print("Sending message")
sent_bytes = clientsocket.send(msg)
print("Sent {0} bytes of the message".format(sent_bytes))
