"""
scenarios.py - Labeled benchmark scenarios for RootScout evaluation.

Structure mirrors OpenRCA's difficulty tiers:
  easy   (task_1–task_3):  single-service, clear signal
  medium (task_4–task_6):  2-3 service cascade
  hard   (task_7–task_10): multi-service, ambiguous / red herrings

Each scenario includes:
  - topology: services + directed edges (caller -> callee)
  - fault_injection: which service fails, how, at what timestamp
  - observed_service: the service where the alert fires (may differ from root cause)
  - ground_truth: root cause component + concise reason + fault start time
  - scoring_points: OpenRCA format string used by evaluate.py
"""

from datetime import datetime, timezone

# Fixed reference time so scoring_points datetimes are deterministic
_BASE_TS = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
_BASE_STR = _BASE_TS.strftime("%Y-%m-%d %H:%M:%S")


def _scoring_points(component: str, reason: str, dt_str: str) -> str:
    return (
        f"The only predicted root cause component is {component}\n"
        f"The only predicted root cause reason is {reason}\n"
        f"The only root cause occurrence time is within 1 minutes "
        f"(i.e., <=1min) of {dt_str}"
    )


# ---------------------------------------------------------------------------
# EASY scenarios (task_1 – task_3): single service, unambiguous failure
# ---------------------------------------------------------------------------

SCENARIO_001 = {
    "id": "scenario_001",
    "task_index": "task_1",
    "difficulty": "easy",
    "title": "Cart-service database connection pool exhausted",
    "description": (
        "The cart-service repeatedly fails to acquire a DB connection "
        "because the connection pool is fully saturated. "
        "frontend and auth-service are healthy."
    ),
    "topology": {
        "services": ["frontend", "auth-service", "cart-service", "database"],
        "edges": [
            ("frontend", "cart-service"),
            ("frontend", "auth-service"),
            ("cart-service", "database"),
            ("auth-service", "database"),
        ],
    },
    "fault_injection": {
        "root_cause_service": "cart-service",
        "fault_type": "db_timeout",
        "error_message": "DatabaseTimeoutError: Connection pool exhausted (pool_size=10, active=10, idle=0)",
        "status_code_http": "504",
        "propagates_to": ["frontend"],
    },
    "observed_service": "frontend",
    "fault_start_ts": _BASE_TS,
    "ground_truth": {
        "root_cause_component": "cart-service",
        "root_cause_reason": "database connection pool exhausted",
    },
    "scoring_points": _scoring_points(
        "cart-service",
        "database connection pool exhausted",
        _BASE_STR,
    ),
}

SCENARIO_002 = {
    "id": "scenario_002",
    "task_index": "task_2",
    "difficulty": "easy",
    "title": "Payment-service out-of-memory crash",
    "description": (
        "The payment-service process crashes with OOM. "
        "All payment calls fail immediately. Other services are healthy."
    ),
    "topology": {
        "services": ["api-gateway", "payment-service", "fraud-service"],
        "edges": [
            ("api-gateway", "payment-service"),
            ("payment-service", "fraud-service"),
        ],
    },
    "fault_injection": {
        "root_cause_service": "payment-service",
        "fault_type": "oom_crash",
        "error_message": "OutOfMemoryError: Java heap space - GC overhead limit exceeded",
        "status_code_http": "503",
        "propagates_to": ["api-gateway"],
    },
    "observed_service": "api-gateway",
    "fault_start_ts": _BASE_TS,
    "ground_truth": {
        "root_cause_component": "payment-service",
        "root_cause_reason": "out-of-memory crash",
    },
    "scoring_points": _scoring_points(
        "payment-service",
        "out-of-memory crash",
        _BASE_STR,
    ),
}

SCENARIO_003 = {
    "id": "scenario_003",
    "task_index": "task_3",
    "difficulty": "easy",
    "title": "Notification-service invalid API key",
    "description": (
        "The notification-service was deployed with a rotated API key that "
        "was never updated in the config. Every outbound call returns 401. "
        "All other services are unaffected."
    ),
    "topology": {
        "services": ["order-service", "notification-service", "email-gateway"],
        "edges": [
            ("order-service", "notification-service"),
            ("notification-service", "email-gateway"),
        ],
    },
    "fault_injection": {
        "root_cause_service": "notification-service",
        "fault_type": "auth_error",
        "error_message": "AuthenticationError: 401 Unauthorized - invalid or expired API key",
        "status_code_http": "401",
        "propagates_to": ["order-service"],
    },
    "observed_service": "order-service",
    "fault_start_ts": _BASE_TS,
    "ground_truth": {
        "root_cause_component": "notification-service",
        "root_cause_reason": "invalid API key in configuration",
    },
    "scoring_points": _scoring_points(
        "notification-service",
        "invalid API key in configuration",
        _BASE_STR,
    ),
}

# ---------------------------------------------------------------------------
# MEDIUM scenarios (task_4 – task_6): 2-3 service cascades
# ---------------------------------------------------------------------------

SCENARIO_004 = {
    "id": "scenario_004",
    "task_index": "task_4",
    "difficulty": "medium",
    "title": "Auth-service latency spike cascades to checkout",
    "description": (
        "auth-service p99 latency spikes to 8s due to an unindexed DB query "
        "introduced in the latest deploy. checkout-service times out waiting "
        "for auth validation; frontend shows 504s. "
        "cart-service is healthy."
    ),
    "topology": {
        "services": ["frontend", "checkout-service", "auth-service", "cart-service", "user-db"],
        "edges": [
            ("frontend", "checkout-service"),
            ("frontend", "cart-service"),
            ("checkout-service", "auth-service"),
            ("auth-service", "user-db"),
        ],
    },
    "fault_injection": {
        "root_cause_service": "auth-service",
        "fault_type": "high_latency",
        "error_message": "SlowQueryWarning: query took 7842ms - missing index on users.session_token",
        "status_code_http": "504",
        "propagates_to": ["checkout-service", "frontend"],
    },
    "observed_service": "frontend",
    "fault_start_ts": _BASE_TS,
    "ground_truth": {
        "root_cause_component": "auth-service",
        "root_cause_reason": "slow database query due to missing index",
    },
    "scoring_points": _scoring_points(
        "auth-service",
        "slow database query due to missing index",
        _BASE_STR,
    ),
}

SCENARIO_005 = {
    "id": "scenario_005",
    "task_index": "task_5",
    "difficulty": "medium",
    "title": "Shared database overloaded by product-service full-table scan",
    "description": (
        "product-service was deployed with a regression that performs a "
        "full-table scan on the shared products DB. "
        "This saturates DB connections so cart-service and order-service "
        "both begin timing out. frontend sees cascading errors."
    ),
    "topology": {
        "services": ["frontend", "cart-service", "order-service", "product-service", "products-db"],
        "edges": [
            ("frontend", "cart-service"),
            ("frontend", "order-service"),
            ("frontend", "product-service"),   # product-service must be reachable so it appears in context
            ("cart-service", "products-db"),
            ("order-service", "products-db"),
            ("product-service", "products-db"),
        ],
    },
    "fault_injection": {
        "root_cause_service": "product-service",
        "fault_type": "db_overload",
        "error_message": "WARN: full table scan on products table - 1.2M rows, no WHERE index used",
        "status_code_http": "500",
        "propagates_to": ["cart-service", "order-service", "frontend"],
    },
    "observed_service": "frontend",
    "fault_start_ts": _BASE_TS,
    "ground_truth": {
        "root_cause_component": "product-service",
        "root_cause_reason": "full table scan saturating shared database connections",
    },
    "scoring_points": _scoring_points(
        "product-service",
        "full table scan saturating shared database connections",
        _BASE_STR,
    ),
}

SCENARIO_006 = {
    "id": "scenario_006",
    "task_index": "task_6",
    "difficulty": "medium",
    "title": "Kafka consumer lag in event-processor degrades downstream",
    "description": (
        "event-processor stopped consuming Kafka messages after a "
        "NullPointerException in its deserialization logic. "
        "recommendation-service and analytics-service, both of which "
        "depend on processed events, start returning stale or empty results."
    ),
    "topology": {
        "services": ["api-gateway", "recommendation-service", "analytics-service", "event-processor", "kafka"],
        "edges": [
            ("api-gateway", "recommendation-service"),
            ("api-gateway", "analytics-service"),
            ("recommendation-service", "event-processor"),
            ("analytics-service", "event-processor"),
            ("event-processor", "kafka"),
        ],
    },
    "fault_injection": {
        "root_cause_service": "event-processor",
        "fault_type": "consumer_crash",
        "error_message": "NullPointerException in EventDeserializer.deserialize() - consumer stopped",
        "status_code_http": "500",
        "propagates_to": ["recommendation-service", "analytics-service"],
    },
    "observed_service": "api-gateway",
    "fault_start_ts": _BASE_TS,
    "ground_truth": {
        "root_cause_component": "event-processor",
        "root_cause_reason": "consumer crash due to deserialization NullPointerException",
    },
    "scoring_points": _scoring_points(
        "event-processor",
        "consumer crash due to deserialization NullPointerException",
        _BASE_STR,
    ),
}

# ---------------------------------------------------------------------------
# HARD scenarios (task_7 – task_10): multi-service, red herrings, ambiguity
# ---------------------------------------------------------------------------

SCENARIO_007 = {
    "id": "scenario_007",
    "task_index": "task_7",
    "difficulty": "hard",
    "title": "Inventory service returns incorrect stock — red herring at frontend",
    "description": (
        "frontend and cart-service both show elevated error rates. "
        "A recent deploy to inventory-service introduced a bug returning "
        "stock=0 for all items, causing checkout to abort with 'item unavailable'. "
        "Metrics on cart-service look fine at first glance — it is inventory-service "
        "returning bad data that is the true root cause."
    ),
    "topology": {
        "services": ["frontend", "cart-service", "inventory-service", "checkout-service", "warehouse-db"],
        "edges": [
            ("frontend", "cart-service"),
            ("frontend", "checkout-service"),
            ("checkout-service", "inventory-service"),
            ("inventory-service", "warehouse-db"),
        ],
    },
    "fault_injection": {
        "root_cause_service": "inventory-service",
        "fault_type": "bad_data",
        "error_message": "WARN: inventory query returned stock_count=0 for all SKUs - possible query regression",
        "status_code_http": "200",  # returns 200 but wrong data
        "propagates_to": ["checkout-service", "frontend"],
        "red_herring": "frontend also logs 4xx errors but they are downstream of the bad inventory data",
    },
    "observed_service": "frontend",
    "fault_start_ts": _BASE_TS,
    "ground_truth": {
        "root_cause_component": "inventory-service",
        "root_cause_reason": "deployment regression returning incorrect zero stock count",
    },
    "scoring_points": _scoring_points(
        "inventory-service",
        "deployment regression returning incorrect zero stock count",
        _BASE_STR,
    ),
}

SCENARIO_008 = {
    "id": "scenario_008",
    "task_index": "task_8",
    "difficulty": "hard",
    "title": "Shipping-service version regression on international orders",
    "description": (
        "A new release of shipping-service introduced a locale parsing bug "
        "that only affects international order addresses (non-US ZIP codes). "
        "order-service shows intermittent 500s (~30%). "
        "Domestic orders succeed; only international ones fail silently. "
        "shipping-service itself reports no errors on health check."
    ),
    "topology": {
        "services": ["api-gateway", "order-service", "shipping-service", "address-validator", "shipping-db"],
        "edges": [
            ("api-gateway", "order-service"),
            ("order-service", "shipping-service"),
            ("shipping-service", "address-validator"),
            ("shipping-service", "shipping-db"),
        ],
    },
    "fault_injection": {
        "root_cause_service": "shipping-service",
        "fault_type": "version_regression",
        "error_message": "ParseError: invalid postal code format for locale en_GB - expected 6 alphanumeric chars",
        "status_code_http": "500",
        "propagates_to": ["order-service", "api-gateway"],
        "red_herring": "address-validator logs show warnings but is functioning correctly",
    },
    "observed_service": "api-gateway",
    "fault_start_ts": _BASE_TS,
    "ground_truth": {
        "root_cause_component": "shipping-service",
        "root_cause_reason": "locale parsing regression in new version for international addresses",
    },
    "scoring_points": _scoring_points(
        "shipping-service",
        "locale parsing regression in new version for international addresses",
        _BASE_STR,
    ),
}

SCENARIO_009 = {
    "id": "scenario_009",
    "task_index": "task_9",
    "difficulty": "hard",
    "title": "Config-service stale cache causes multi-service feature-flag failure",
    "description": (
        "config-service's Redis cache became stale after a cache flush. "
        "It started serving default (disabled) feature flags instead of "
        "the live values. Three services (payment-service, search-service, "
        "recommendation-service) suddenly stopped executing premium code paths, "
        "causing widespread user-visible degradation. "
        "Each of the three downstream services logs its own apparent 'config read' errors."
    ),
    "topology": {
        "services": ["api-gateway", "payment-service", "search-service", "recommendation-service", "config-service", "redis"],
        "edges": [
            ("api-gateway", "payment-service"),
            ("api-gateway", "search-service"),
            ("api-gateway", "recommendation-service"),
            ("payment-service", "config-service"),
            ("search-service", "config-service"),
            ("recommendation-service", "config-service"),
            ("config-service", "redis"),
        ],
    },
    "fault_injection": {
        "root_cause_service": "config-service",
        "fault_type": "stale_cache",
        "error_message": "WARN: Redis MISS on all feature-flag keys - serving defaults (redis FLUSHALL detected)",
        "status_code_http": "200",
        "propagates_to": ["payment-service", "search-service", "recommendation-service"],
        "red_herring": "all three downstream services emit config errors making each look like root cause",
    },
    "observed_service": "api-gateway",
    "fault_start_ts": _BASE_TS,
    "ground_truth": {
        "root_cause_component": "config-service",
        "root_cause_reason": "stale Redis cache serving default feature flags after cache flush",
    },
    "scoring_points": _scoring_points(
        "config-service",
        "stale Redis cache serving default feature flags after cache flush",
        _BASE_STR,
    ),
}

SCENARIO_010 = {
    "id": "scenario_010",
    "task_index": "task_10",
    "difficulty": "hard",
    "title": "Rate-limiter misconfiguration causes intermittent 429s across platform",
    "description": (
        "A misconfigured rate-limiter in api-gateway was set to 10 req/s globally "
        "instead of 10 req/s per user. During peak traffic all downstream services "
        "see intermittent 429 errors. Each service logs the rejections as its own "
        "upstream errors. search-service, cart-service, and user-service all appear "
        "to be failing. The actual root cause is the api-gateway rate-limit config."
    ),
    "topology": {
        "services": ["api-gateway", "search-service", "cart-service", "user-service", "rate-limiter"],
        "edges": [
            ("api-gateway", "search-service"),
            ("api-gateway", "cart-service"),
            ("api-gateway", "user-service"),
            ("api-gateway", "rate-limiter"),
        ],
    },
    "fault_injection": {
        "root_cause_service": "api-gateway",
        "fault_type": "config_misconfiguration",
        "error_message": "RateLimitExceeded: global limit 10 req/s reached - should be per-user - config key rate_limit_mode=global",
        "status_code_http": "429",
        "propagates_to": ["search-service", "cart-service", "user-service"],
        "red_herring": "multiple downstream services all show error spikes simultaneously",
    },
    "observed_service": "api-gateway",
    "fault_start_ts": _BASE_TS,
    "ground_truth": {
        "root_cause_component": "api-gateway",
        "root_cause_reason": "rate limiter misconfigured with global limit instead of per-user limit",
    },
    "scoring_points": _scoring_points(
        "api-gateway",
        "rate limiter misconfigured with global limit instead of per-user limit",
        _BASE_STR,
    ),
}

# ---------------------------------------------------------------------------
# Master list — ordered easy -> medium -> hard
# ---------------------------------------------------------------------------

SYNTHETIC_SCENARIOS = [
    SCENARIO_001,
    SCENARIO_002,
    SCENARIO_003,
    SCENARIO_004,
    SCENARIO_005,
    SCENARIO_006,
    SCENARIO_007,
    SCENARIO_008,
    SCENARIO_009,
    SCENARIO_010,
]

ALL_SCENARIOS = SYNTHETIC_SCENARIOS  # OpenRCA CSV cases are appended at runtime by openrca_loader.py
