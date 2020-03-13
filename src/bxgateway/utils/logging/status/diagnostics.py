from dataclasses import dataclass
from bxgateway.utils.logging.status.analysis import Analysis
from bxgateway.utils.logging.status.summary import Summary


@dataclass
class Diagnostics:
    summary: Summary
    analysis: Analysis
