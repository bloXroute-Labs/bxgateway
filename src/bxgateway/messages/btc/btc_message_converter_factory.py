from bxgateway.messages.btc.btc_normal_message_converter import BtcNormalMessageConverter
from bxgateway.messages.btc.btc_extension_message_converter import BtcExtensionMessageConverter


def create_btc_message_converter(magic, opts):
    if opts.use_extensions:
        return BtcExtensionMessageConverter(magic)
    else:
        return BtcNormalMessageConverter(magic)
