from aiohttp.web import Response


class ResponseFormatter:
    _response: Response

    def __init__(self, response: Response):
        self._response = response

    def __repr__(self) -> str:
        response = self._response
        return f"HTTPResponse <status: {response.status}, body: {response.body}>"
