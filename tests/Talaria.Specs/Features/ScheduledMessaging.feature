Feature: Scheduled Messaging
  As a developer
  I want to schedule commands and events for future delivery via IMessageBus
  So that time-delayed actions and notifications are delivered only when due

  Scenario: Scheduling a command by delay delivers it after the delay
    Given a Talaria scheduled messaging host with an in-memory deferral store
    And a command handler registered for ShipOrder
    When I schedule a ShipOrder command for order "ORD-100" with a delay of 120 ms
    Then the ShipOrder command handler should receive order "ORD-100" once due

  Scenario: Scheduling an event by timestamp delivers it once due
    Given a Talaria scheduled messaging host with an in-memory deferral store
    And an event handler registered for OrderPlaced
    When I schedule an OrderPlaced event for order "ORD-200" due in 120 ms
    Then the OrderPlaced event handler should receive order "ORD-200" once due

  Scenario: Scheduled command is not delivered before its due time and is delivered once due
    Given a Talaria scheduled messaging host with an in-memory deferral store
    And a command handler registered for ShipOrder
    When I schedule a ShipOrder command for order "ORD-300" with a delay of 300 ms
    Then the ShipOrder command handler should not have received order "ORD-300" within 100 ms
    And the ShipOrder command handler should receive order "ORD-300" once due
