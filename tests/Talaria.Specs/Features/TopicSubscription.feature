Feature: Topic Subscription
  As a developer
  I want to subscribe to a message topic using MapTopic
  So that I can process messages as they arrive

  Scenario: Simple message consumption
    Given a Talaria host with an in-memory transport
    And a handler registered for topic "orders.placed"
    When a message of type OrderPlaced is published to "orders.placed"
    Then the handler should be invoked with the OrderPlaced message

  Scenario: Handler receives message headers
    Given a Talaria host with an in-memory transport
    And an envelope-aware handler registered for topic "orders.placed"
    When a message with trace context is published to "orders.placed"
    Then the handler should receive the message with trace headers

  Scenario: Multiple handlers on different topics
    Given a Talaria host with an in-memory transport
    And a handler registered for topic "orders.placed"
    And a handler registered for topic "payments.completed"
    When a message is published to "orders.placed"
    And a message is published to "payments.completed"
    Then each handler should process only its own topic messages
