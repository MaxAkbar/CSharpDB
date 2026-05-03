# Security Policy

## Supported Versions
We currently support security fixes for:
- `main` branch (latest code)

## Reporting a Vulnerability
Please do not report security vulnerabilities through public GitHub issues.

Instead, report issues by one of the following methods:
- Preferred: GitHub Private Vulnerability Reporting (Security tab)

Include as much of the following as possible:
- A description of the vulnerability and impact
- Steps to reproduce (proof-of-concept if available)
- Affected versions/commit hash
- Any relevant logs or screenshots

## Response Timeline
We will acknowledge receipt of your report within 72 hours and will provide a status update within 7 days when possible.

## Coordinated Disclosure
We ask that you keep the vulnerability confidential until we have released a fix or have agreed on a public disclosure timeline.

## Remote API Security
Remote REST and daemon gRPC access support opt-in API-key authentication.

Configuration keys:

- `CSharpDB:Daemon:Security:Mode=ApiKey`
- `CSharpDB:Daemon:Security:ApiKey`
- `CSharpDB:Daemon:Security:ApiKeyHeaderName`

The default mode is `None` for backward compatibility. API-key mode is a shared-secret guard for private deployments; it is not JWT, RBAC, mTLS, or a replacement for TLS termination and network access control.

## Data At Rest
CSharpDB database and WAL files are plaintext today. At-rest encryption remains roadmap work and should not be assumed for regulated or hostile-host deployments.
