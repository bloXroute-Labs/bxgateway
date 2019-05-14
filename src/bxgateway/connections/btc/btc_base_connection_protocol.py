import bxgateway.messages.btc.btc_message_converter_factory as converter_factory
from bxgateway import btc_constants
from bxgateway.connections.abstract_blockchain_connection_protocol import AbstractBlockchainConnectionProtocol
from bxgateway.messages.btc.addr_btc_message import AddrBtcMessage
from bxgateway.messages.btc.btc_message_factory import btc_message_factory
from bxgateway.messages.btc.btc_message_type import BtcMessageType
from bxgateway.messages.btc.ping_btc_message import PingBtcMessage
from bxgateway.messages.btc.pong_btc_message import PongBtcMessage
from bxgateway.messages.btc.version_btc_message import VersionBtcMessage


class BtcBaseConnectionProtocol(AbstractBlockchainConnectionProtocol):
    def __init__(self, connection):
        super(BtcBaseConnectionProtocol, self).__init__(connection)

        self.magic = connection.node.opts.blockchain_net_magic
        self.version = connection.node.opts.blockchain_version

        connection.hello_messages = btc_constants.BTC_HELLO_MESSAGES
        connection.header_size = btc_constants.BTC_HDR_COMMON_OFF
        connection.message_factory = btc_message_factory
        connection.message_converter = converter_factory.create_btc_message_converter(
            self.magic,
            connection.node.opts
        )
        connection.message_handlers = {
            BtcMessageType.PING: self.msg_ping,
            BtcMessageType.PONG: self.msg_pong,
            BtcMessageType.GET_ADDRESS: self.msg_getaddr
        }
        connection.ping_message = PingBtcMessage(self.magic)

        # Establish connection with blockchain node
        version_msg = VersionBtcMessage(self.magic, self.version, connection.peer_ip, connection.peer_port,
                                        connection.node.opts.external_ip, connection.node.opts.external_port,
                                        connection.node.opts.blockchain_nonce, 0,
                                        str(connection.node.opts.protocol_version).encode("utf-8"),
                                        connection.node.opts.blockchain_services)
        connection.enqueue_msg(version_msg)

    def msg_ping(self, msg):
        """
        Handle ping messages. Respond with a pong.
        """
        reply = PongBtcMessage(self.magic, msg.nonce())
        self.connection.enqueue_msg(reply)

    def msg_pong(self, msg):
        """
        Handle pong messages. For now, don't do anything since we don't ping.
        """
        pass

    def msg_getaddr(self, _msg):
        """
        Handle a getaddr message. Return a blank address to preserve privacy.
        """
        reply = AddrBtcMessage(self.magic)
        self.connection.enqueue_msg(reply)
