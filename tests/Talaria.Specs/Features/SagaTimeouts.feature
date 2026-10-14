Feature: Saga Timeouts
  In order to handle SLA deadlines and missing follow-up events in long-running workflows
  As a developer using Talaria
  I want sagas to request and cancel scheduled timeouts atomically with state transitions

  Scenario: Unanswered saga timeout fires once due and completes the saga
    Given a payment saga configured with a 150 ms payment timeout
    When an order "ORD-TIMEOUT-1" starts the payment saga
    Then the payment saga for "ORD-TIMEOUT-1" should be in state "AwaitingPayment"
    And once the payment timeout is due the saga for "ORD-TIMEOUT-1" should be completed
    And a payment timeout notification for "ORD-TIMEOUT-1" should have been recorded

  Scenario: Completing the saga before the timeout is due suppresses the late timeout
    Given a payment saga configured with a 150 ms payment timeout
    When an order "ORD-TIMEOUT-2" starts the payment saga
    And payment is completed for order "ORD-TIMEOUT-2" before the timeout is due
    Then the payment saga for "ORD-TIMEOUT-2" should be completed
    And waiting 250 ms past the timeout deadline should not invoke the timeout handler for "ORD-TIMEOUT-2"

  Scenario: Explicitly canceling a timeout during an active transition suppresses the late timeout
    Given a payment saga configured with a 150 ms payment timeout
    When an order "ORD-TIMEOUT-3" starts the payment saga
    And payment is authorized for order "ORD-TIMEOUT-3" cancelling the payment timeout
    Then the payment saga for "ORD-TIMEOUT-3" should be in state "PaymentAuthorized"
    And waiting 250 ms past the timeout deadline should not invoke the timeout handler for "ORD-TIMEOUT-3"
    And the payment saga for "ORD-TIMEOUT-3" should remain in state "PaymentAuthorized"
