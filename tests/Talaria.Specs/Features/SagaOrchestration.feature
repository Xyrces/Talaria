Feature: Saga Orchestration
  In order to implement complex, long-running processes without stateful coupling
  As a developer using Talaria
  I want to configure sagas as pure functions that process messages and update state

  Scenario: Completing a saga through sequential messages
    Given a configured saga "OrderSaga"
    When I publish an "OrderPlaced" message with correlation ID "ord-123"
    And wait 100 ms
    Then the saga state for "ord-123" should exist
    When I publish an "OrderBilled" message with correlation ID "ord-123"
    And wait 100 ms
    Then the saga state for "ord-123" should no longer exist
    And an "OrderCompleted" message should be dispatched

  Scenario: Handling out-of-order messages via deferral
    Given a configured saga "OrderSaga"
    When I publish an "OrderBilled" message with correlation ID "ord-456"
    And wait 100 ms
    Then the saga state for "ord-456" should not exist
    When I publish an "OrderPlaced" message with correlation ID "ord-456"
    And wait 500 ms
    Then the saga state for "ord-456" should no longer exist
    And an "OrderCompleted" message should be dispatched

  Scenario: Trace context is propagated from saga stimulus to output messages
    Given a configured saga "OrderSaga"
    When I publish an "OrderPlaced" message with correlation ID "ord-prop"
    And wait 100 ms
    When I publish an "OrderBilled" message with correlation ID "ord-prop" and traceparent "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    And wait 100 ms
    Then an "OrderCompleted" message should be dispatched with traceparent "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
