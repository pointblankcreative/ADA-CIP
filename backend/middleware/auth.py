"""Auth middleware — attaches user identity to the request.

Identity comes from Cloud Run IAP when present: Google's proxy injects
X-Goog-Authenticated-User-Email ("accounts.google.com:user@domain.com")
on every authenticated request, and IAP is the gate in front of the
service (CLAUDE.md §4.4), so the header is trustworthy in production.
Locally (no IAP) we fall back to the dev stub so request.state.user is
always populated.

Once Firebase credentials are configured, this can additionally validate
the Authorization: Bearer <id_token> header.
"""

import logging

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

logger = logging.getLogger(__name__)

PUBLIC_PATHS = {"/health", "/docs", "/openapi.json", "/redoc"}

IAP_EMAIL_HEADER = "X-Goog-Authenticated-User-Email"


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

        iap_email = request.headers.get(IAP_EMAIL_HEADER, "")
        if iap_email:
            # Header format is "accounts.google.com:user@domain.com" — keep
            # just the address.
            email = iap_email.split(":", 1)[-1]
            request.state.user = {"uid": email, "email": email}
        else:
            request.state.user = {
                "uid": "dev-user",
                "email": "dev@pointblankcreative.com",
            }
        return await call_next(request)
