# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it responsibly.

**Do not open a public issue.**

Instead, please use [GitHub's private vulnerability reporting](https://github.com/lmoloney/OpenMirroringFaker/security/advisories/new).

Include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

You should receive an acknowledgment within 48 hours and a resolution timeline within 7 days.

## Scope

This policy covers the Open Mirroring Faker CLI tool code. It does **not** cover:
- Azure services (OneLake, Fabric) — report those to [Microsoft Security Response Center](https://msrc.microsoft.com/)

## Security Best Practices

When using this tool:
- **Never commit `.env` files** — use `.env.example` as a template
- **Use `az login` or Managed Identity** instead of storing credentials
- **Restrict OneLake access** to only the identities that need it
