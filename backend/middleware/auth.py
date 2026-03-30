"""Firebase Auth middleware — stub that passes all requests through.

Once Firebase credentials are configured, this will validate the
Authorization: Bearer <id_token> header and attach user info to the request.
"""

import logging

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

logger = logging.getLogger(__name__)

PUBLIC_PATHS = {"/health", "/docs", "/openapi.json", "/redoc"}


class FirebaseAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        if request.url.path in PUBLIC_PATHS:
            return await call_next(request)

        # TODO: validate Firebase ID token from Authorization header
        # token = request.headers.get("Authorization", "").removeprefix("Bearer ")
        # if not token:
        #     return JSONResponse(status_code=401, content={"detail": "Missing auth"})
        # decoded = firebase_admin.auth.verify_id_token(token)
        # request.state.user = decoded

        request.state.user = {"uid": "dev-user", "email": "dev@pointblankcreative.com"}
        return await call_next(request)
