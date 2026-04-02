Feature: Dead Letter Queue
  As a system operator
  I want failed messages to be routed to dead-letter queues
  So that I can investigate and replay them

  Scenario: Failed messages route to topic-specific DLQ
    Given a Talaria host with an in-memory transport
    And a handler for "orders.placed" that always throws
    When a message is published to "orders.placed"
    And the handler has attempted to process the message
    Then the message should appear in "orders.placed.dlq"

  Scenario: Failed messages also route to application-wide DLQ
    Given a Talaria host with an in-memory transport
    And a handler for "orders.placed" that always throws
    When a message is published to "orders.placed"
    And the handler has attempted to process the message
    Then the message should appear in the application-wide DLQ

  Scenario: Hop count exceeded routes to DLQ
    Given a Talaria host with an in-memory transport and max hop count of 3
    And a handler registered for topic "orders.placed"
    When a message with hop count 3 is published to "orders.placed"
    And the handler has attempted to process the message
    Then the message should appear in "orders.placed.dlq"
    And the handler should not have been invoked
