import unittest

from bxgateway.utils import configuration_utils
from bxgateway.models.config.gateway_node_config_model import GatewayNodeConfigModel
from bxgateway import gateway_constants
from bxcommon.test_utils.mocks.mock_node import MockNode
from bxcommon.test_utils import helpers


class ConfigToolsTests(unittest.TestCase):
    def setUp(self):
        self.node = MockNode(helpers.get_common_opts(8888))

    def test_update_node_config_update_value(self):
        old_value = self.node.opts.throughput_stats_interval
        new_value = 90
        self.assertFalse(old_value == new_value)
        configuration_utils.compare_and_update(new_value,
                           self.node.opts.throughput_stats_interval,
                           item="throughput_stats_interval",
                           setter=lambda val: self.node.opts.__setattr__("throughput_stats_interval", val))
        self.assertEqual(self.node.opts.throughput_stats_interval, new_value)

    def test_update_node_config_ignore_missing_new_value(self):
        old_value = self.node.opts.throughput_stats_interval
        new_value = None
        self.assertIsNotNone(old_value)
        configuration_utils.compare_and_update(new_value,
                           self.node.opts.throughput_stats_interval,
                           item="throughput_stats_interval",
                           setter=lambda val: self.node.opts.__setattr__("throughput_stats_interval", val))
        self.assertEqual(self.node.opts.throughput_stats_interval, old_value)

    def test_read_file(self):
        node_config_model = configuration_utils.read_config_file(gateway_constants.CONFIG_FILE_NAME)
        self.assertIsInstance(node_config_model, GatewayNodeConfigModel)
        node_config_model = configuration_utils.read_config_file("NotAFileName.json")
        self.assertIsInstance(node_config_model, GatewayNodeConfigModel)
