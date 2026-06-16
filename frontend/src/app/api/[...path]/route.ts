// Server-side reverse proxy for the backend API.
//
// The browser calls this frontend's OWN same-origin /api/* path (which is
// covered by the frontend's IAP), and we forward each request to the real
// backend with a Google-minted Cloud Run identity token. That lets the backend
// be locked to "require authentication" plus this frontend's service account,
// which closes the public-backend hole, with no DNS and no cross-origin auth
// problem.
//
// Pre-lock (backend still public) it works fine without a token; once the
// backend requires auth, the identity token from the Cloud Run metadata server
// is what gets the proxy through. Locally there is no metadata server, so it
// falls back to an unauthenticated call against the dev backend.

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

// The real backend URL. NEXT_PUBLIC_API_URL is set both at build (Dockerfile
// ENV / cloudbuild) and at runtime (Cloud Run env var) to the backend service
// URL; only this server-side proxy reads it now. Force https for the deployed
// backend (env has it as http) so neither the ID-token audience nor a followed
// redirect ever lands on an http URL.
let BACKEND_URL = (process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000").replace(/\/+$/, "");
if (BACKEND_URL.startsWith("http://") && !BACKEND_URL.includes("localhost")) {
  BACKEND_URL = "https://" + BACKEND_URL.slice("http://".length);
}

// Cache the identity token per instance (Cloud Run ID tokens last ~1 hour).
let tokenCache: { token: string; expiresAt: number } | null = null;

async function getIdToken(audience: string): Promise<string | null> {
  const now = Date.now();
  if (tokenCache && tokenCache.expiresAt - now > 5 * 60 * 1000) {
    return tokenCache.token;
  }
  try {
    const res = await fetch(
      "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience=" +
        encodeURIComponent(audience),
      { headers: { "Metadata-Flavor": "Google" }, cache: "no-store" },
    );
    if (!res.ok) return null;
    const token = (await res.text()).trim();
    if (!token) return null;
    tokenCache = { token, expiresAt: now + 50 * 60 * 1000 };
    return token;
  } catch {
    // No metadata server (local dev / build) — the dev backend needs no token.
    return null;
  }
}

async function proxy(request: Request): Promise<Response> {
  const url = new URL(request.url);
  const target = `${BACKEND_URL}${url.pathname}${url.search}`;

  const headers = new Headers(request.headers);
  headers.delete("host");
  headers.delete("content-length");
  headers.delete("connection");
  headers.delete("cookie"); // don't leak the frontend's IAP session to the backend
  headers.delete("x-goog-iap-jwt-assertion"); // frontend-only IAP assertion

  const token = await getIdToken(BACKEND_URL);
  if (token) headers.set("authorization", `Bearer ${token}`);

  const hasBody = request.method !== "GET" && request.method !== "HEAD";

  let upstream: Response;
  try {
    upstream = await fetch(target, {
      method: request.method,
      headers,
      body: hasBody ? await request.arrayBuffer() : undefined,
      // Follow the backend's own redirects (FastAPI's trailing-slash 307s and
      // any http->https upgrade) SERVER-SIDE, so the browser never receives an
      // absolute http:// backend redirect that it would block as mixed content.
      redirect: "follow",
      cache: "no-store",
    });
  } catch {
    return new Response("Upstream request failed", { status: 502 });
  }

  const responseHeaders = new Headers(upstream.headers);
  // The fetch above already decoded the body; drop length/encoding so the
  // browser doesn't try to re-decode it or mismatch the length.
  responseHeaders.delete("content-encoding");
  responseHeaders.delete("content-length");
  responseHeaders.delete("transfer-encoding");

  return new Response(upstream.body, {
    status: upstream.status,
    statusText: upstream.statusText,
    headers: responseHeaders,
  });
}

export const GET = proxy;
export const POST = proxy;
export const PUT = proxy;
export const PATCH = proxy;
export const DELETE = proxy;
export const HEAD = proxy;
