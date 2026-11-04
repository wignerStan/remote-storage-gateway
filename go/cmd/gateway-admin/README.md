# Gateway operator maintenance

`gateway-admin tls-sync` obtains the configured Tailscale certificate, publishes
the existing gateway configuration and its explicit secret-file allowlist, then
restarts and waits for the gateway Deployment. It uses Infra process execution
without a shell. Certificates, private keys, provider credentials and Secret JSON
are never printed or put in command arguments.

```sh
gateway-admin tls-sync --domain incus.tail5ced8c.ts.net \
  --config /etc/storage-gateway/gateway.json \
  --secrets-dir /etc/storage-gateway/secrets \
  --state-dir /var/lib/storage-gateway \
  --instance k3s-cilium --namespace selfhost \
  --secret storage-gateway --deployment storage-gateway
```

The configuration and secret directories must be private. Configured runtime
secret paths must be `/run/gateway-secrets/<basename>`; the operator reads those
exact basenames from `--secrets-dir`. Private files require owner-only permissions.
The certificate is written beside the operator config as `tls.crt`; the key is
`secrets/tls-key`. The existing configuration must reference those mounted TLS
files. No provider configuration is created or changed.

A lock serializes runs. A SHA-256 of the complete destination-bound Secret payload
is recorded atomically only after apply, rollout restart and rollout completion
succeed. An unchanged payload skips Kubernetes mutations. Any partial failure
keeps the old successful fingerprint so the next run retries. Schedule this
one-shot command every 12 hours using the host's service manager.
