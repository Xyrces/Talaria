## Summary

<!-- One-paragraph description of what this PR changes and why.
     Link the issue this closes with "Closes #<id>" or "Refs #<id>". -->

Closes #

## Type of change

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking additive change)
- [ ] Breaking change (fix or feature that would change existing behavior)
- [ ] Documentation / repo hygiene (no production-code change)

## Public-API surface

- [ ] No public-surface change
- [ ] Additive only — new types/methods/optional parameters documented
- [ ] Breaking change discussed in an issue and called out in this PR

<!-- For additive changes, list the new symbols and link their XML docs: -->

## Tests

- [ ] SpecFlow feature added/updated under `tests/Talaria.Specs/`
- [ ] Unit test added/updated alongside the production code
- [ ] Integration test added/updated (Kafka / Redis / AppHost)
- [ ] Existing tests still pass locally

## Verification commands run

<!--
Tick the commands you ran locally before pushing. CI runs all of these.
-->

- [ ] `dotnet build Talaria.slnx --configuration Release --no-restore` — 0 warnings, 0 errors
- [ ] `dotnet test Talaria.slnx --configuration Release --no-build`
- [ ] `dotnet format Talaria.slnx --verify-no-changes`
- [ ] `dotnet list package --vulnerable --include-transitive`

## Compliance checks

- [ ] New/modified `.cs` files under `src/` start with `// SPDX-License-Identifier: Apache-2.0`
- [ ] New public/protected members carry XML doc comments (`summary`, parameter notes, `since` where appropriate)
- [ ] No new dependencies introduced; or, if introduced, they are Apache-2.0-compatible and discussed here

## Notes for reviewers

<!-- Anything reviewers should pay extra attention to: race conditions,
     lock ordering, idempotency contract, public-API contract, licensing impact. -->

## Risk and rollback

<!-- What happens if this is merged and needs to be reverted? Is there a
     feature flag, a follow-up issue, or a migration note for users? -->
