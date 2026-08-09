---
name: Bug report
about: Report a defect, incorrect behavior, or unexpected exception in Talaria
title: "[bug] "
labels: ["bug", "triage"]
assignees: []
---

## Summary

<!-- One-paragraph description of what went wrong. -->

## Reproduction steps

1.
2.
3.

## Expected behavior

<!-- What you expected to happen. -->

## Actual behavior

<!-- What actually happened, including any exception traces. -->

## Environment

| Item            | Value                          |
| --------------- | ------------------------------ |
| Talaria version | <!-- e.g. 0.3.0 or git SHA --> |
| .NET runtime    | <!-- e.g. .NET 9.0.5 -->       |
| OS              | <!-- e.g. Ubuntu 24.04 -->     |
| Transport       | <!-- Kafka / InMemory / other --> |
| State store     | <!-- Redis / InMemory / other --> |
| Deployment      | <!-- Aspire AppHost / standalone / container --> |

## Configuration

<!--
Relevant `TalariaOptions` / `KafkaTransportOptions` / `TalariaRedisOptions`
values. Redact secrets.
-->

## Logs / traces / exception

<!-- Paste the exception trace, OpenTelemetry span, or relay log lines.
     If the trace contains payload data, prefer to redact and describe. -->

## Impact

<!-- How severe is this? Data loss, double-publish, DLQ, performance, etc. -->

## Workaround

<!-- If you found a workaround, describe it. -->
