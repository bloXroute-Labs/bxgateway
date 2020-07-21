from bxcommon.utils.init_task import AbstractInitTask
from bxcommon.services import sdn_http_service
from bxcommon.common_opts import CommonOpts

from bxutils.services.node_ssl_service import NodeSSLService


class SetAccountInfo(AbstractInitTask):
    def action(self, opts: CommonOpts, node_ssl_service: NodeSSLService) -> None:
        account_id = node_ssl_service.get_account_id()
        if account_id:
            # TODO: use local cache for account_model
            account_model = sdn_http_service.fetch_account_model(account_id)
            if account_model:
                opts.set_account_options(account_model)


class ValidateNetworkOpts(AbstractInitTask):
    def action(self, opts: CommonOpts, node_ssl_service: NodeSSLService) -> None:
        opts.validate_network_opts()


gateway_node_init_tasks = [ValidateNetworkOpts(SetAccountInfo())]
