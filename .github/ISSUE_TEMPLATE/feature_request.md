---
name: Feature request
about: Propose a new capability, configuration knob, or API addition to Talaria
title: "[feat] "
labels: ["enhancement", "triage"]
assignees: []
---

## Problem

<!-- What user-facing problem does this solve? Frame it as a story
     ("as a Saga operator, I want ...") rather than a solution. -->

## Proposed solution

<!-- What you would like Talaria to expose — a method, an option, a metric,
     a behavior. Sketch the API surface (signatures, XML doc summary) if
     you have one in mind. -->

## Alternatives considered

<!-- Other ways to address the problem, including not addressing it at all.
     This is the place to flag backward-compatibility or licensing-impact
     concerns. -->

## Public-API impact

- [ ] Pure internal change (no public-surface delta)
- [ ] Additive only (new overload, new optional parameter, default interface member)
- [ ] Changes an existing signature or wire format — **please flag explicitly** and link an issue discussing the breaking change

## Outbox / deferral / transport impact

<!-- Does this touch the outbox relay, the deferral sweeper, the transport,
     or the idempotency gate? Each of these has its own concurrency contract;
     flag if your change crosses one. -->

## Telemetry

<!-- Will you add a new metric, span, or trace attribute?
     Follow the `talaria.<area>.<name>` convention. -->

## Test plan

<!-- Which SpecFlow scenario under `tests/Talaria.Specs/` will you add?
     Which unit tests? Which integration test (Kafka / Redis / AppHost)? -->

## Licensing notes

<!-- Anything that affects the commercial offering, the Apache-2.0
     contribution license, or included third-party code (e.g. generated
     files, large asset dumps). -->
