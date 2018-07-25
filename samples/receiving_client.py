import socket

CLIENT_IP = '127.0.0.1'
CLIENT_PORT = 9001
# Must be >= the message length
MAX_MESSAGE_LEN = 4096

# Setup socket and connect
clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
clientsocket.connect((CLIENT_IP, CLIENT_PORT))

# Receiving message
print("Receiving message")
msg = clientsocket.recv(MAX_MESSAGE_LEN)
print("First 100 bytes of received message: " + repr(msg[:100]))
