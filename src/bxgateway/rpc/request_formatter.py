from aiohttp.web import Request


class RequestFormatter:
    _request: Request

    def __init__(self, request: Request):
        self._request = request

    def __repr__(self) -> str:
        return f"HTTPRequest <{self._request.headers}>"
