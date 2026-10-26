# Security

## Reporting a vulnerability

Email **security@spiffyvault.net**. Please do not open a public issue for a
security problem.

We reply within **48 hours** to say we have your report, and then keep you
told as we work on it. Tell us:

- what you found and where (package, file, version or commit);
- how to reproduce it, and what an attacker gains;
- whether you want to be credited, and under what name.

We publish an advisory once a fixed version is out. Please give us a
reasonable time to fix a problem before you disclose it.

## Scope

- `@go-ricochet/udx` and `@go-ricochet/libp2p-udx` (this repository).
- Releases are published only from this repository's `publish` workflow,
  with npm provenance. A release without a provenance statement did not
  come from us. The exceptions are the first releases,
  `@go-ricochet/udx` 0.1.0 and `@go-ricochet/libp2p-udx` 0.1.1, published
  by hand: npm lets a workflow publish a package only after the package
  exists.
