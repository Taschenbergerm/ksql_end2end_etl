import datetime
import time

from loguru import logger
from starlette.requests import Headers
from starlette.requests import Request
from starlette.responses import Response
from starlette_context import context
from starlette.middleware.base import BaseHTTPMiddleware

from ksql_streaming.middleware import custom_headers


NECESSARY_HEADERS = [
    "X-User-Email",
    "X-Forwarded-IP",
    "X-Request-Id",
    "X-Request-Timestamp",
]


class TimerMiddleware(BaseHTTPMiddleware):

    async def dispatch(self, request: Request, call_next)-> Response:
        start = datetime.datetime.now()
        timer = time.perf_counter()
        response = await call_next(request)
        response.headers[custom_headers.TIMESTAMP] = f"{start.isoformat()}"
        elapsed_time = time.perf_counter() - timer
        response.headers[custom_headers.ELAPSED_TIME] = f"{elapsed_time:.6f} seconds"
        return response


class HeaderCheckMiddleware(BaseHTTPMiddleware):

    async def dispatch(self, request: Request, call_next) -> Response:
        request_id = context.data[custom_headers.REQUEST_Id]
        response = await call_next(request)
        response.headers[custom_headers.REQUEST_Id] = request_id
        missing_headers = await self.check_headers(request.headers)
        if missing_headers:
            logger.warning(f"{context.data[custom_headers.REQUEST_Id]} | Missing headers: {missing_headers}")
            response.headers[
                custom_headers.WARNINGS
            ] = f"Missing headers: {missing_headers}"
        return response

    async def check_headers(self, request_header: Headers) -> list:
        return [header for header in NECESSARY_HEADERS if header not in request_header]
