def escape_label_value(value: str) -> str:
    """PromQL and LogQL share the same label-matcher string syntax, and both
    PrometheusMetricsCollector and LokiLogsCollector interpolate
    primaryService/environment (which originate from Alertmanager labels via
    Connector Service - untrusted input) into a query language - escape them
    the same way any string reaching a query language should be."""
    return value.replace("\\", "\\\\").replace('"', '\\"')
