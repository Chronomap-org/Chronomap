# Security Policy

## Supported Versions

ChronoMap is pre-1.0-in-spirit (currently on a 3.x line predating a formal
stability guarantee). Security fixes are made against the latest release
on `main`; older tags are not backported.

| Version | Supported          |
| ------- | ------------------ |
| 3.x     | :white_check_mark: |
| < 3.0   | :x:                |

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Instead, report it privately using one of these channels:

- GitHub: use the **[Report a vulnerability](../../security/advisories/new)**
  button under the Security tab (preferred, because it keeps discussion
  private until a fix ships).
- Email: devansh.jay.singh@gmail.com with a description and, if possible,
  a minimal reproduction.

You should get an acknowledgement within **5 business days**. If the
report is confirmed, a fix and a coordinated disclosure timeline will be
worked out with you before any public advisory is published. If it's
declined, you'll get an explanation why.

## Scope

ChronoMap is an in-memory data structure with no network I/O, so most
"vulnerabilities" in the traditional sense (RCE, injection, auth bypass)
don't apply. Relevant reports include things like:

- Deserialization issues in `load_json` / `load_pickle` (especially
  pickle, which is unsafe by design; see the docstring warning).
- Denial-of-service via unbounded memory growth that isn't already
  documented/expected behavior.
- Thread-safety bugs that could corrupt data across concurrent access.
