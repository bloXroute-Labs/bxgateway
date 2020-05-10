from bxgateway.messages.ont.ont_normal_consensus_message_converter import OntNormalConsensusMessageConverter


def create_ont_consensus_message_converter(magic, opts):
    if opts.use_extensions or opts.import_extensions:
        from bxgateway.messages.ont.ont_extension_consensus_message_converter import OntExtensionConsensusMessageConverter

    if opts.use_extensions:
        return OntExtensionConsensusMessageConverter(magic)
    else:
        return OntNormalConsensusMessageConverter(magic)
