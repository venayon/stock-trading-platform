# ADR 0015 — Enforce validation on the server and preview it in the client

Date: 2026-10-24 · Status: Accepted for the validation slice by user direction

## Context

The owner requested completing all trade-field validation in one batch, client-side validation and Swagger API
documentation. MongoDB and Kafka adapters are not implemented. Existing designs describe eventual trade acceptance with
broker metadata; a validation preview must not imply those side effects happened.

## Decision

Centralize canonical field validation in `TradeValidator`, invoked by `TradeService` before any persistence or
publication. Expose a side-effect-free `POST /api/trades/validate` that returns a normalized preview. Serve a small
same-origin browser form with immediate client feedback and independently enforce the rules at the API boundary. Publish
local Swagger UI and generated OpenAPI through springdoc 2.8.17, compatible with Boot 3.5. Runtime dependencies remain
Java 21 and Boot 3.5.16.

Reject malformed JSON and coercions before domain validation. Use BigDecimal on the server and string/BigInt arithmetic
in the client, preserving decimal prices and 64-bit quantities. Return field errors as ProblemDetail. Document omitted
currency versus explicit null. The initial currency allowlist is USD/GBP/EUR; configuration-driven expansion is deferred
and must update the contract and client together.

## Alternatives and consequences

Client-only validation improves feedback but can be bypassed by any direct HTTP client; rejected as the enforcement
boundary. Server-only validation remains correct but delays field feedback; runner-up if maintaining a form is no longer
useful. Bean Validation alone is useful for simple required/range checks, but centralized domain validation was chosen
to handle normalization, exact decimal representability and aggregation without duplicating business rules across
request DTOs and service calls.

Client checks mirror server rules and need regression tests as contracts evolve. Swagger exposes a machine-readable
contract and examples; it does not guarantee comprehensive browser-side validation. The preview endpoint has no
repository/publisher dependency and cannot manufacture trade acceptance. The future create endpoint must reuse the
validator while adding idempotency, persistence, publication state and failure recovery from ADR 0005.

## Evidence

64 Java test cases and 28 client test cases pass. Packaged app returns HTTP 200 for a valid preview, 400 with field
errors for invalid input, and serves Swagger UI, OpenAPI and the client form. No visual browser verification was
available. Full trade creation remains out of scope for this slice.
