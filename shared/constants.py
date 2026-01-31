"""Centralized constants"""

# Redis TTLs
REDIS_KEY_TTL_SECONDS = 7 * 24 * 60 * 60  # 7 days

# Timeouts
DEFAULT_NODE_TIMEOUT_SECONDS = 300  # 5 minutes
MAX_NODE_TIMEOUT_SECONDS = 3600     # 1 hour
MIN_NODE_TIMEOUT_SECONDS = 1        # 1 second

# Limits
MAX_NODES_PER_WORKFLOW = 1000
MAX_CONFIG_SIZE_BYTES = 10 * 1024   # 10KB
MAX_TEMPLATE_LENGTH = 500

# Retry Configuration
MAX_RETRY_ATTEMPTS = 3
INITIAL_RETRY_DELAY_SECONDS = 1
MAX_RETRY_DELAY_SECONDS = 60

# Retryable HTTP Status Codes
RETRYABLE_HTTP_STATUS_CODES = {500, 502, 503, 504, 408, 429}

# External Handlers (that should support retry)
EXTERNAL_HANDLERS = {"http_request", "call_external_service", "llm_service"}

# Deterministic Handlers (no retry needed)
DETERMINISTIC_HANDLERS = {"compute", "transform", "aggregate"}

# Allowed Handler Types (whitelist)
ALLOWED_HANDLER_TYPES = EXTERNAL_HANDLERS | DETERMINISTIC_HANDLERS
