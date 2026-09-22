# Food Order Observability Lab

Food delivery simulation instrumented with FastAPI and OpenTelemetry. It exports metrics, logs and traces through OTLP to an OpenTelemetry Collector, then uses Prometheus, Loki, Tempo and Grafana.

## Endpoints

- `GET /health`
- `GET /menu`
- `POST /orders`
- `GET /orders/{order_id}`
- `POST /demo/traffic?count=20`
- Interactive API: `/docs`

## Example order

```json
{"customer":"Leonardo","item":"pizza","quantity":2,"scenario":"normal"}
```

Scenarios: `normal`, `payment_failure`, `slow_kitchen`, `delivery_error`.

## OpenShift

Import this Git repository using its Dockerfile, choose Deployment, target port 8000 and create a secure Route. The existing `otel-collector` Service is discovered by DNS.

Generate traffic:

```bash
ROUTE="https://$(oc get route food-order-api -o jsonpath='{.spec.host}')"
curl -sk -X POST "$ROUTE/demo/traffic?count=25"
```
