from bxgateway.messages.ont.ont_no_compression_message_converter import OntNoCompressionMessageConverter
from bxgateway.messages.ont.ont_normal_message_converter import OntNormalMessageConverter


def create_ont_message_converter(magic):
    return OntNormalMessageConverter(magic)
