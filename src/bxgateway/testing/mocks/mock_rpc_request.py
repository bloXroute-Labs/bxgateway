import json
from typing import Dict, Any, Optional


class MockRPCRequest:

    def __init__(self, headers: Dict[str, Any], json_body: str, text: Optional[str] = None):
        self.headers = headers
        self._json_body = json_body
        if text is None:
            text = json_body
        self._text = text

    async def text(self) -> str:
        assert self._text is not None
        return self._text

    async def json(self) -> Dict[str, Any]:
        return json.loads(self._json_body)
