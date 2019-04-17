from bxgateway.messages.btc.btc_normal_message_converter import BtcNormalMessageConverter


def create_btc_message_converter(magic, opts):
    if opts.use_extensions or opts.import_extensions:
        from bxgateway.messages.btc.btc_extension_message_converter import BtcExtensionMessageConverter

    if opts.use_extensions:
        return BtcExtensionMessageConverter(magic)
    else:
        return BtcNormalMessageConverter(magic)
