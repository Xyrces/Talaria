Feature: Saga Compensation Workflows
  In order to safely unwind distributed side effects when a multi-step saga fails or times out
  As a developer using Talaria
  I want declarative sequential and batch compensation workflows with durable state and late-arrival handling

  Scenario: Sequential multi-step compensation executes in strict LIFO order waiting for each acknowledgment
    Given an order fulfillment saga configured with sequential compensation
    When order "ORD-COMP-SEQ-1" reserves inventory "RES-1" and charges payment "CHG-1"
    And shipment booking fails for order "ORD-COMP-SEQ-1"
    Then only "RefundPayment" should have been dispatched for order "ORD-COMP-SEQ-1"
    And "ReleaseInventory" should not yet be dispatched for order "ORD-COMP-SEQ-1"
    And "OrderFailed" should not yet be published for order "ORD-COMP-SEQ-1"
    When "PaymentRefunded" acknowledgment arrives for order "ORD-COMP-SEQ-1"
    Then "ReleaseInventory" should be dispatched for order "ORD-COMP-SEQ-1"
    And "OrderFailed" should not yet be published for order "ORD-COMP-SEQ-1"
    When "InventoryReleased" acknowledgment arrives for order "ORD-COMP-SEQ-1"
    Then "OrderFailed" should be published for order "ORD-COMP-SEQ-1"
    And the order fulfillment saga state for "ORD-COMP-SEQ-1" should be completed and purged

  Scenario: Single-phase batch compensation stages all applicable compensating commands atomically and completes
    Given an order fulfillment saga configured with single-phase batch compensation
    When order "ORD-COMP-BATCH-1" reserves inventory "RES-B1" and charges payment "CHG-B1"
    And shipment booking fails triggering batch compensation for order "ORD-COMP-BATCH-1"
    Then both "RefundPayment" and "ReleaseInventory" and "OrderFailed" should be dispatched for order "ORD-COMP-BATCH-1"
    And the order fulfillment saga state for "ORD-COMP-BATCH-1" should be completed and purged

  Scenario: Late forward event arriving during active sequential compensation is captured and compensated
    Given an order fulfillment saga configured with sequential compensation and late payment arrival handling
    When order "ORD-COMP-LATE-1" reserves inventory "RES-L1"
    And shipment booking fails for order "ORD-COMP-LATE-1"
    Then only "ReleaseInventory" should have been dispatched for order "ORD-COMP-LATE-1"
    When a late "PaymentSucceeded" event with charge "CHG-LATE-1" arrives for order "ORD-COMP-LATE-1" while compensating
    And "InventoryReleased" acknowledgment arrives for order "ORD-COMP-LATE-1"
    Then "RefundPayment" should be dispatched for order "ORD-COMP-LATE-1"
    When "PaymentRefunded" acknowledgment arrives for order "ORD-COMP-LATE-1"
    Then "OrderFailed" should be published for order "ORD-COMP-LATE-1"
    And the order fulfillment saga state for "ORD-COMP-LATE-1" should be completed and purged
