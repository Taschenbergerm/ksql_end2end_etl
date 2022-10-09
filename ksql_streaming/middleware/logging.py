from loguru import logger
from starlette.requests import Request
from starlette.responses import Response
from starlette_context import context
from starlette.middleware.base import BaseHTTPMiddleware

from ksql_streaming.middleware import custom_headers


class LoggerMiddleware(BaseHTTPMiddleware):

    async def dispatch(self, request: Request, call_next) -> Response:
        request_id = context.data[custom_headers.REQUEST_Id]
        if "/rest" in request.url.path:
            await self.log_request(request_id, request)

        response = await call_next(request)
        return response

    async def log_request(self,request_id: int, request: Request) -> None:

        logger.info(f"{request_id} | {request.url.path}")
        logger.info(f"{request_id} | {request.headers}")
        logger.info(f"{request_id} | {request.path_params}")
        logger.info(f"{request_id} | {request.query_params}")