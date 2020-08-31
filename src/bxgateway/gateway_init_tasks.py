from bxcommon.services import sdn_http_service
from bxcommon.common_opts import CommonOpts
from bxcommon import common_init_tasks

from bxutils.services.node_ssl_service import NodeSSLService


def set_account_info(opts: CommonOpts, node_ssl_service: NodeSSLService) -> None:
    account_id = node_ssl_service.get_account_id()
    if account_id:
        # TODO: use local cache for account_model
        account_model = sdn_http_service.fetch_account_model(account_id)
        if account_model:
            opts.set_account_options(account_model)


def validate_network_opts(opts: CommonOpts, _node_ssl_service: NodeSSLService) -> None:
    opts.validate_network_opts()


init_tasks = [
    set_account_info,
    validate_network_opts,
    common_init_tasks.set_network_info,
    common_init_tasks.set_node_model,
]
