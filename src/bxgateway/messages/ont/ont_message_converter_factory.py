from bxgateway.messages.ont.ont_normal_message_converter import OntNormalMessageConverter


def create_ont_message_converter(magic, opts):
    if opts.use_extensions or opts.import_extensions:
        from bxgateway.messages.ont.ont_extension_message_converter import OntExtensionMessageConverter

    if opts.use_extensions:
        return OntExtensionMessageConverter(magic)
    else:
        return OntNormalMessageConverter(magic)
