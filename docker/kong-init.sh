#!/bin/sh
# Kong Admin API bootstrap — registers services, routes, and plugins.
# Runs once after Kong is healthy. All operations are idempotent (|| true).

KONG_ADMIN="http://kong:8001"

echo "Bootstrapping Kong..."

# ── order-service upstream ──────────────────────────────────────────
curl -sf -o /dev/null -X PUT "$KONG_ADMIN/services/order-service" \
  -d "url=http://order-service:8080" || true

curl -sf -o /dev/null -X PUT "$KONG_ADMIN/routes/order-api" \
  -d "service.name=order-service" \
  -d "paths[]=/orders" \
  -d "strip_path=false" || true

# Rate-limiting on all order-service traffic
curl -sf -o /dev/null -X POST "$KONG_ADMIN/services/order-service/plugins" \
  -d "name=rate-limiting" \
  -d "config.minute=60" \
  -d "config.hour=1000" \
  -d "config.policy=local" || true

# ── correlation-id globally (all routes) ────────────────────────────
curl -sf -o /dev/null -X POST "$KONG_ADMIN/plugins" \
  -d "name=correlation-id" \
  -d "config.header_name=X-Correlation-Id" \
  -d "config.generator=uuid#counter" \
  -d "config.echo_downstream=true" || true

echo "Kong bootstrap complete."