#
# Copyright (C) 2017, bloXroute Labs, All rights reserved.
# See the file COPYING for details.
#
# Client node code
#

from bxcommon.btc_messages import *
from bxcommon.connections import *
from bxcommon.messages import *
from bxcommon.utils import *


# A bloXroute client
class Client(AbstractClient):
    def __init__(self, server_ip, server_port, servers, node_addr, node_params):
        super(Client, self).__init__(server_ip, server_port)

        self.servers = servers  # A list of (ip, port) pairs of other bloXroute servers
        self.idx = 0

        self.node_addr = node_addr  # The address of the blockchain node this client is connected to
        self.node_params = node_params
        self.node_conn = None  # Connection object for the blockchain node

        self.node_msg_queue = deque()

        log_verbose("initialized node state")

    # Create a NodeConnection object from this Node to (ip, port)
    def create_node_conn(self, ip, port):
        ip = socket.gethostbyname(ip)
        log_debug("Connecting to " + ip + ":" + str(port))

        # init_client_socket will do all of the work given the ip address.
        self.init_client_socket(BTCNodeConnection, ip, port, setup=True)

    # Broadcasts message msg to every connection except requester.
    def broadcast(self, msg, sender):
        log_debug("Broadcasting message from sender {0}".format(sender))

        for conn in self.connection_pool:
            if conn.state & ConnectionState.ESTABLISHED and conn != sender:
                conn.enqueue_msg(msg)

    # Sends a message to the node that this is connected to
    def send_bytes_to_node(self, msg):
        if self.node_conn is not None:
            log_debug("Sending message to node: " + repr(msg))
            self.node_conn.enqueue_msg_bytes(msg)
        else:
            log_debug("Adding things to node's message queue")
            self.node_msg_queue.append(msg)

    def can_retry_after_desroy(self, teardown, conn):
        # If the connection is to a bloXroute server, then retry it unless we're tearing down the Node
        return not teardown and conn.is_server

    def get_connection_class(self, ip=None):
        if ip is None:
            return ServerConnection
        else:
            return BTCNodeConnection if self.node_addr[0] == ip else ServerConnection

    def connect_to_peers(self):
        for idx in self.servers:
            ip, port = self.servers[idx]
            log_debug("connecting to relay node {0}:{1}".format(ip, port))
            self.create_conn(ip, port)

        self.create_node_conn(self.node_addr[0], self.node_addr[1])


class Connection(AbstractConnection):
    def __init__(self, sock, address, node, from_me=False, setup=False):

        super(Connection, self).__init__(sock, address, node, from_me, setup)

        log_debug("initialized connection to {0}".format(self.peer_desc))

        self.is_server = False  # This isn't a server message

        # Command to message handler for that function.
        self.message_handlers = None

    # Receives and processes the next bytes on the socket's inputbuffer.
    # Returns 0 in order to avoid being rescheduled if this was an alarm.
    def _recv(self, msg_cls, hello_msgs):
        self.collect_input()

        while True:
            if self.state & ConnectionState.MARK_FOR_CLOSE:
                return 0

            is_full_msg, msg_type, payload_len = msg_cls.peek_message(self.inputbuf)

            log_debug("Message is full: {0}".format(is_full_msg))

            if not is_full_msg:
                break

            # Full messages must be a version or verack if the connection isn't established yet.
            msg = self.pop_next_message(payload_len)
            # If there was some error in parsing this message, then continue the loop.
            if msg is None:
                if self.num_bad_messages == MAX_BAD_MESSAGES:
                    log_debug("Got enough bad messages! Marking connection from {0} closed".format(self.peer_desc))
                    self.state |= ConnectionState.MARK_FOR_CLOSE
                    return 0  # I have MAX_BAD_MESSAGES messages that failed to parse in a row.

                self.num_bad_messages += 1
                continue

            self.num_bad_messages = 0

            if not (self.state & ConnectionState.ESTABLISHED) and msg_type not in hello_msgs:
                log_err("Connection to {0} not established and got {1} message!  Closing."
                        .format(self.peer_desc, msg_type))
                self.state |= ConnectionState.MARK_FOR_CLOSE
                return 0

            log_debug("Received message of type {0} from {1}".format(msg_type, self.peer_desc))

            if msg_type in self.message_handlers:
                msg_handler = self.message_handlers[msg_type]
                msg_handler(msg)

        log_debug("Done receiving from {0}".format(self.peer_desc))
        return 0

    #################
    # Sending Logic #
    #################

    # Enqueues the contents of a Message instance, msg, to our outputbuf and attempts to send it if the underlying
    #   socket has room in the send buffer.
    def enqueue_msg(self, msg):
        if self.state & ConnectionState.MARK_FOR_CLOSE:
            return

        self.outputbuf.enqueue_msgbytes(msg.rawbytes())

        if self.sendable:
            self.send()

    # Enqueues the raw bytes of a message, msg_bytes, to our outputbuf and attempts to send it if the underlying socket
    #   has room in the send buffer.
    def enqueue_msg_bytes(self, msg_bytes):
        if self.state & ConnectionState.MARK_FOR_CLOSE:
            return

        size = len(msg_bytes)

        log_debug("Adding message of length {0} to {1}'s outputbuf".format(size, self.peer_desc))

        self.outputbuf.enqueue_msgbytes(msg_bytes)

        if self.sendable:
            self.send()

    # Send some bytes to the peer of this connection from the next cut through message or from the outputbuffer.
    def send(self):
        if self.state & ConnectionState.MARK_FOR_CLOSE:
            return

        byteswritten = self.send_bytes_on_buffer(self.outputbuf)
        log_debug("{0} bytes sent to {1}. {2} bytes left.".format(byteswritten, self.peer_desc, self.outputbuf.length))

    def close(self):
        log_debug("Closing connection to {0}".format(self.peer_desc))
        self.sock.close()

    # Dumps state using log_debug
    def dump_state(self):
        log_debug("Connection {0} state dump".format(self.peer_desc))
        log_debug("Connection state: {0}".format(self.state))

        log_debug("Inputbuf size: {0}".format(self.inputbuf.length))
        log_debug("Outputbuf size: {0}".format(self.outputbuf.length))


class ServerConnection(Connection):
    def __init__(self, sock, address, node, from_me=False, setup=False):
        Connection.__init__(self, sock, address, node, setup=setup)
        self.is_server = True
        self.is_persistent = True

        hello_msg = HelloMessage(self.node.idx)
        self.enqueue_msg(hello_msg)

        # Command to message handler for that function.
        self.message_handlers = {
            'hello': self.msg_hello,
            'ack': self.msg_ack,
            'broadcast': self.msg_broadcast,
            'txassign': self.msg_txassign,
            'tx': self.msg_tx
        }

    def recv(self):
        return Connection._recv(self, Message, ['hello', 'ack'])

    ###
    # Handlers for each message type
    ###

    # Handle a broadcast message
    def msg_broadcast(self, msg):
        blx_block = BCHMessageParsing.broadcastmsg_to_block(msg, self.node.tx_manager)
        if blx_block is not None:
            log_debug("Decoded block successfully- sending block to node")
            self.node.send_bytes_to_node(blx_block)
        else:
            log_debug("Failed to decode blx block. Dropping")

    # Receive a transaction from the bloXroute network.
    def msg_tx(self, msg):
        hash_val = BTCObjectHash(sha256(sha256(msg.blob()).digest()).digest(), length=HASH_LEN)

        if hash_val != msg.msg_hash():
            log_err("Got ill formed tx message from the bloXroute network")
            return

        log_debug("Adding hash value to tx manager and forwarding it to node")
        self.node.tx_manager.hash_to_contents[hash_val] = msg.blob()
        # XXX: making a copy here- should make this more efficient
        # XXX: maybe by sending the full tx message in a block...
        # XXX: this should eventually be moved into the parser.
        buf = bytearray(BCH_HDR_COMMON_OFF) + msg.blob()
        if self.node.node_conn is not None:
            txmsg = BTCMessage(self.node.node_conn.magic, 'tx', len(msg.blob()), buf)
            self.node.send_bytes_to_node(txmsg.rawbytes())

    # Receive a transaction assignment from txhash -> shortid
    def msg_txassign(self, msg):
        log_debug("Processing txassign message")
        if self.node.tx_manager.get_txid(msg.tx_hash()) == -1:
            log_debug("Assigning {0} to sid {1}".format(msg.tx_hash(), msg.short_id()))
            self.node.tx_manager.assign_tx_to_sid(msg.tx_hash(), msg.short_id(), time.time())


# XXX: flesh out this class a bit more to handle transactions as well.
# Utils for message parsing for Bitcoin utils
class BCHMessageParsing(object):
    # Convert a block message to a broadcast message
    @staticmethod
    def block_to_broadcastmsg(msg, tx_manager):
        # Do the block compression
        size = 0
        buf = deque()
        header = msg.header()
        size += len(header)
        buf.append(header)

        for tx in msg.txns():
            tx_hash = BTCObjectHash(buf=sha256(sha256(tx).digest()).digest(), length=HASH_LEN)
            shortid = tx_manager.get_txid(tx_hash)
            if shortid == -1:
                buf.append(tx)
                size += len(tx)
            else:
                next_tx = bytearray(5)
                log_debug("XXX: Packing transaction with shortid {0} into block".format(shortid))
                struct.pack_into('<I', next_tx, 1, shortid)
                buf.append(next_tx)
                size += 5

        # Parse it into the bloXroute message format and send it along
        block = bytearray(size)
        off = 0
        for blob in buf:
            next_off = off + len(blob)
            block[off:next_off] = blob
            off = next_off

        return BroadcastMessage(msg.block_hash(), block)

    # Convert a block message to a broadcast message
    @staticmethod
    def broadcastmsg_to_block(msg, tx_manager):
        # XXX: make this not a copy
        blob = bytearray(msg.blob())

        size = 0
        pieces = deque()

        # get header size
        headersize = 80 + BCH_HDR_COMMON_OFF
        _, txn_count_size = btcvarint_to_int(blob, headersize)
        headersize += txn_count_size

        header = blob[:headersize]
        pieces.append(header)
        size += headersize

        off = size
        while off < len(blob):
            if blob[off] == 0x00:
                sid, = struct.unpack_from('<I', blob, off + 1)
                tx = tx_manager.get_tx_from_sid(sid)
                if tx is None:
                    log_err("XXX: Failed to decode transaction with short id {0} received from bloXroute".format(sid))
                    return None
                off += 5
            else:
                txsize = get_next_tx_size(blob, off)
                tx = blob[off:off + txsize]
                off += txsize

            pieces.append(tx)
            size += len(tx)

        blx_block = bytearray(size)
        off = 0
        for piece in pieces:
            next_off = off + len(piece)
            blx_block[off:next_off] = piece
            off = next_off

        return blx_block


# XXX: change BTC to BCH...
# Connection from a bloXroute client to a BCH blockchain node
class BTCNodeConnection(Connection):
    ESTABLISHED = 0b1

    NONCE = random.randint(0, sys.maxint)  # Used to detect connections to self.

    def __init__(self, sock, address, node, setup=False, from_me=False):
        Connection.__init__(self, sock, address, node, setup=setup)

        self.is_persistent = True
        magic_net = node.node_params['magic']
        self.magic = magic_dict[magic_net] if magic_net in magic_dict else int(magic_net)
        self.services = int(node.node_params['services'])
        self.version = node.node_params['version']
        self.protocol_version = int(node.node_params['protocol_version'])

        if 'nonce' not in node.node_params:
            node.node_params['nonce'] = random.randint(0, sys.maxint)

        self.nonce = node.node_params['nonce']

        # I must be the one that is establishing this connection.
        version_msg = VersionBTCMessage(self.magic, self.protocol_version, self.peer_ip, self.peer_port, self.my_ip,
                                        self.my_port, self.nonce, 0, self.version, self.services)
        self.enqueue_msg(version_msg)

        # Command to message handler for that function.
        self.message_handlers = {
            'ping': self.msg_ping,
            'pong': self.msg_pong,
            'version': self.msg_version,
            'block': self.msg_block,
            'tx': self.msg_tx,
            'getaddr': self.msg_getaddr,
            'inv': self.msg_inv,
        }

    def pop_next_message(self, payload_len, msg_type=Message, hdr_size=HDR_COMMON_OFF):
        return super(BTCNodeConnection, self).pop_next_message(payload_len, BTCMessage, BCH_HDR_COMMON_OFF)

    def recv(self):
        return Connection._recv(self, BTCMessage, ['version', 'verack'])

    ###
    # Handlers for each message type
    ###

    # Process ping message and send a pong.
    def msg_ping(self, msg):
        reply = PongBTCMessage(self.magic, msg.nonce())
        self.enqueue_msg(reply)

    # Ignore pong messages since we never send a ping.
    def msg_pong(self, msg):
        pass

    # Process incoming version message.
    # We are the node that initiated this connection, so we do not check for misbehavior.
    # Record that we received the version message, send a verack and synchronize chains if need be.
    def msg_version(self, msg):
        self.state |= ConnectionState.ESTABLISHED
        reply = VerAckBTCMessage(self.magic)
        self.enqueue_msg(reply)

        if self.state & ConnectionState.ESTABLISHED == ConnectionState.ESTABLISHED:
            for msg in self.node.node_msg_queue:
                self.enqueue_msg(msg)

            if self.node.node_msg_queue:
                self.node.node_msg_queue = deque()

            self.node.node_conn = self

    # Reply to GetAddr message with a blank Addr message to preserve privacy.
    def msg_getaddr(self, msg):
        reply = AddrBTCMessage(self.magic)
        self.enqueue_msg(reply)

    # Since I am only connected to this one node, we assume that everything in this inv
    # message is new and we want the data
    def msg_inv(self, msg):
        getdata = GetDataBTCMessage(magic=msg.magic(), inv_vects=[x for x in msg])
        self.enqueue_msg(getdata)

    # Handle a tx message by broadcasting this to the entire network.
    def msg_tx(self, msg):
        blx_txmsg = TxMessage(msg.tx_hash(), msg.tx())

        # All connections outside of this one is a bloXroute server
        log_debug("Broadcasting the transaction to peers")
        self.node.broadcast(blx_txmsg, self)
        self.node.tx_manager.hash_to_contents[msg.tx_hash()] = msg.tx()

    # Handle a block message.
    def msg_block(self, msg):
        blx_blockmsg = BCHMessageParsing.block_to_broadcastmsg(msg, self.node.tx_manager)
        log_debug("Compressed block with hash {0} to size {1} from size {2}"
                  .format(msg.block_hash(), len(blx_blockmsg.rawbytes()), len(msg.rawbytes())))
        self.node.broadcast(blx_blockmsg, self)
