from bxgateway.messages.eth.eth_normal_message_converter import EthNormalMessageConverter


def create_eth_message_converter(opts):
    if opts.use_extensions or opts.import_extensions:
        from bxgateway.messages.eth.eth_extension_message_converter import EthExtensionMessageConverter

    # TODO temp - need to remove opts.enable_eth_extensions
    if opts.use_extensions and opts.enable_eth_extensions:
        return EthExtensionMessageConverter()
    else:
        return EthNormalMessageConverter()
