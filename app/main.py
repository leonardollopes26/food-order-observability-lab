import asyncio
import json
import logging
import os
import random
import time
import uuid
from typing import Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode

OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "food-order-api")

resource = Resource.create({
    "service.name": SERVICE_NAME,
    "service.version": "1.0.0",
    "service.namespace": "food-delivery-lab",
    "deployment.environment": os.getenv("DEPLOYMENT_ENVIRONMENT", "lab"),
})

tracer_provider = TracerProvider(resource=resource)
tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=OTEL_ENDPOINT, insecure=True)))
trace.set_tracer_provider(tracer_provider)
tracer = trace.get_tracer(SERVICE_NAME)

metric_reader = PeriodicExportingMetricReader(
    OTLPMetricExporter(endpoint=OTEL_ENDPOINT, insecure=True), export_interval_millis=5000
)
meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
metrics.set_meter_provider(meter_provider)
meter = metrics.get_meter(SERVICE_NAME)

logger_provider = LoggerProvider(resource=resource)
logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter(endpoint=OTEL_ENDPOINT, insecure=True)))
set_logger_provider(logger_provider)

logger = logging.getLogger("food-order")
logger.setLevel(logging.INFO)
logger.addHandler(LoggingHandler(level=logging.INFO, logger_provider=logger_provider))

orders_total = meter.create_counter("food.orders", unit="1")
errors_total = meter.create_counter("food.order.errors", unit="1")
revenue_total = meter.create_counter("food.revenue", unit="EUR")
order_duration = meter.create_histogram("food.order.duration", unit="ms")
payment_duration = meter.create_histogram("food.payment.duration", unit="ms")
kitchen_duration = meter.create_histogram("food.kitchen.duration", unit="ms")
delivery_duration = meter.create_histogram("food.delivery.duration", unit="ms")
status_total = meter.create_counter("food.orders.by_status", unit="1")

app = FastAPI(title="Food Order Observability Lab", version="1.0.0")
FastAPIInstrumentor.instrument_app(app, tracer_provider=tracer_provider)

MENU = {
    "burger": {"name": "Observability Burger", "price": 12.90},
    "pizza": {"name": "Telemetry Pizza", "price": 15.50},
    "pasta": {"name": "Tracing Pasta", "price": 13.40},
    "salad": {"name": "Healthy Metrics Salad", "price": 9.80},
}
ORDERS: dict[str, dict] = {}


class OrderRequest(BaseModel):
    customer: str = Field(min_length=2, max_length=80)
    item: Literal["burger", "pizza", "pasta", "salad"]
    quantity: int = Field(default=1, ge=1, le=10)
    scenario: Literal["normal", "payment_failure", "slow_kitchen", "delivery_error"] = "normal"


def trace_id() -> str:
    return format(trace.get_current_span().get_span_context().trace_id, "032x")


def log_event(level: int, event: str, **fields) -> None:
    logger.log(level, json.dumps({"event": event, "trace_id": trace_id(), **fields}, default=str))


async def stage(name: str, low: float, high: float, order_id: str) -> float:
    started = time.perf_counter()
    with tracer.start_as_current_span(name) as span:
        span.set_attribute("order.id", order_id)
        await asyncio.sleep(random.uniform(low, high))
    return (time.perf_counter() - started) * 1000


@app.get("/health")
async def health():
    return {"status": "healthy", "service": SERVICE_NAME}


@app.get("/menu")
async def menu():
    return MENU


@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    if order_id not in ORDERS:
        raise HTTPException(404, "Order not found")
    return ORDERS[order_id]


@app.post("/orders")
async def create_order(request: OrderRequest):
    started = time.perf_counter()
    order_id = str(uuid.uuid4())[:8]
    price = MENU[request.item]["price"] * request.quantity
    attributes = {"item": request.item, "scenario": request.scenario}

    with tracer.start_as_current_span("order.process") as span:
        span.set_attribute("order.id", order_id)
        span.set_attribute("order.value", price)
        span.set_attribute("order.scenario", request.scenario)
        log_event(logging.INFO, "order.received", order_id=order_id, customer=request.customer, value=price)

        await stage("order.validation", 0.02, 0.08, order_id)
        await stage("inventory.check", 0.03, 0.12, order_id)

        payment_ms = await stage("payment.process", 0.08, 0.25, order_id)
        payment_duration.record(payment_ms, attributes)
        if request.scenario == "payment_failure":
            errors_total.add(1, {**attributes, "stage": "payment"})
            status_total.add(1, {"status": "payment_failed"})
            span.set_status(Status(StatusCode.ERROR, "Payment declined"))
            log_event(logging.ERROR, "payment.declined", order_id=order_id, duration_ms=round(payment_ms, 2))
            raise HTTPException(402, {"message": "Payment declined", "order_id": order_id, "trace_id": trace_id()})

        kitchen_low, kitchen_high = ((2.5, 4.0) if request.scenario == "slow_kitchen" else (0.15, 0.50))
        kitchen_ms = await stage("kitchen.prepare", kitchen_low, kitchen_high, order_id)
        kitchen_duration.record(kitchen_ms, attributes)
        if request.scenario == "slow_kitchen":
            log_event(logging.WARNING, "kitchen.slow", order_id=order_id, duration_ms=round(kitchen_ms, 2))

        delivery_ms = await stage("delivery.dispatch", 0.08, 0.30, order_id)
        delivery_duration.record(delivery_ms, attributes)
        if request.scenario == "delivery_error":
            errors_total.add(1, {**attributes, "stage": "delivery"})
            status_total.add(1, {"status": "delivery_failed"})
            span.set_status(Status(StatusCode.ERROR, "No driver available"))
            log_event(logging.ERROR, "delivery.failed", order_id=order_id, reason="no_driver")
            raise HTTPException(503, {"message": "No driver available", "order_id": order_id, "trace_id": trace_id()})

        total_ms = (time.perf_counter() - started) * 1000
        order = {"order_id": order_id, "status": "accepted", "item": request.item,
                 "quantity": request.quantity, "total_eur": round(price, 2), "duration_ms": round(total_ms, 2),
                 "trace_id": trace_id()}
        ORDERS[order_id] = order
        orders_total.add(1, attributes)
        revenue_total.add(price, {"item": request.item})
        status_total.add(1, {"status": "accepted"})
        order_duration.record(total_ms, attributes)
        log_event(logging.INFO, "order.completed", **order)
        return order


@app.post("/demo/traffic")
async def demo_traffic(count: int = 10):
    count = max(1, min(count, 100))
    results = {"accepted": 0, "failed": 0}
    scenarios = ["normal"] * 7 + ["slow_kitchen", "payment_failure", "delivery_error"]
    for index in range(count):
        req = OrderRequest(customer=f"demo-{index}", item=random.choice(list(MENU)), quantity=random.randint(1, 3), scenario=random.choice(scenarios))
        try:
            await create_order(req)
            results["accepted"] += 1
        except HTTPException:
            results["failed"] += 1
    return results

