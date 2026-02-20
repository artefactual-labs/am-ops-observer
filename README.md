# am-ops-observer

Go service for Archivematica operations with two primary modules:

1. Troubleshooting for running transfers (near real-time)
2. Monthly reporting for customer-facing KPIs

## PoC status (important)

This repository is currently a Proof of Concept (PoC): it demonstrates possible features and workflows, but it is not a production-hardened deployment.

Current assumptions/limits:

- Designed for an all-in-one Archivematica server layout.
- Not validated for multi-pipeline deployments.
- Not validated for separated/remote Storage Service server topologies.
- Cross-host network/auth hardening and scale tuning are out of scope in this PoC phase.
- No authentication/authorization (no login, no RBAC, no tenant isolation).
- No API security controls yet (no TLS termination in-app, no API tokens, no rate limiting).
- No background job queue/scheduler for report generation.
- No alerting/notifications policy integration.
- No migration/versioning workflow for app-owned SQLite data.
- Limited test coverage focused on core handlers; no full end-to-end test suite yet.
- Future work: Keycloak/OIDC SSO integration (login, session handling, protected routes, and role mapping).

## Install packages (DEB/RPM)

### 1) Install package

Debian/Ubuntu:

```bash
sudo dpkg -i am-ops-observer_<version>_amd64.deb
```

RHEL/Rocky/Alma/Fedora:

```bash
sudo rpm -Uvh am-ops-observer-<version>.x86_64.rpm
```

### 1.1) Upgrade package

Debian/Ubuntu:

```bash
sudo dpkg -i am-ops-observer_<new-version>_amd64.deb
```

RHEL/Rocky/Alma/Fedora:

```bash
sudo rpm -Uvh am-ops-observer-<new-version>.x86_64.rpm
```

### 1.2) Uninstall package

Debian/Ubuntu:

```bash
sudo dpkg -r am-ops-observer
```

RHEL/Rocky/Alma/Fedora:

```bash
sudo rpm -e am-ops-observer
```

### 2) Configure files

Edit bootstrap file:

```bash
sudo editor /etc/default/am-ops-observer
```

Edit main non-secret config:

```bash
sudo editor /etc/am-ops-observer/config.env
```

Edit secrets:

```bash
sudo editor /etc/am-ops-observer/secrets.env
sudo chmod 600 /etc/am-ops-observer/secrets.env
```

### 3) Start service

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now am-ops-observer
sudo systemctl status am-ops-observer
```

### 4) Verify

```bash
curl -s http://127.0.0.1:8080/health | jq
curl -s http://127.0.0.1:8080/ready | jq
curl -s http://127.0.0.1:8080/metrics | head -n 40
```

## Configuration quick reference (packaged)

Packaged deployment layout:

- Bootstrap file: `/etc/default/am-ops-observer`
- Main config: `/etc/am-ops-observer/config.env`
- Secrets file: `/etc/am-ops-observer/secrets.env`

Config loading order at startup:

1. Bootstrap defaults from `./am-ops-observer.env` and `/etc/default/am-ops-observer`
2. Main non-secret config from `APP_CONFIG_FILE` (if set), else `/etc/am-ops-observer/config.env`
3. Secrets from `APP_SECRETS_FILE` (if set), else systemd credentials directory (`%d/app-secrets`), else `/etc/am-ops-observer/secrets.env`
4. Real process environment variables (highest priority)

## Configuration (all options)

### Required vs optional options

Legend:

- `Required`: must be set explicitly for intended behavior.
- `Conditional`: required only when a feature is enabled.
- `Optional`: has safe default.

#### Bootstrap and file-path options

| Variable | Required | Default | Notes |
|---|---|---|---|
| `APP_CONFIG_FILE` | Optional | `/etc/am-ops-observer/config.env` | Path to main non-secret env file. |
| `APP_SECRETS_FILE` | Optional | auto from systemd credential; fallback `/etc/am-ops-observer/secrets.env` | Explicit secrets env file path. |
| `APP_SECRETS_CREDENTIAL_NAME` | Optional | `app-secrets` | Credential file name under `CREDENTIALS_DIRECTORY`. |

#### HTTP service options

| Variable | Required | Default | Notes |
|---|---|---|---|
| `APP_LISTEN_ADDR` | Optional | `:8080` | HTTP bind address. |
| `APP_READ_TIMEOUT_SEC` | Optional | `10` | HTTP read timeout. |
| `APP_WRITE_TIMEOUT_SEC` | Optional | `20` | HTTP write timeout. |
| `APP_SHUTDOWN_TIMEOUT_SEC` | Optional | `10` | Graceful shutdown timeout. |
| `APP_DEFAULT_RUNNING_LIMIT` | Optional | `50` | Default list limit for running views. |
| `APP_DEFAULT_CUSTOMER_ID` | Optional | `default` | Default customer id in reports. |

#### MCP database options (read-only)

| Variable | Required | Default | Notes |
|---|---|---|---|
| `APP_DB_ENABLED` | Optional | `false` | Enables MCP-backed endpoints. |
| `APP_DB_HOST` | Conditional | `127.0.0.1` | Required when `APP_DB_ENABLED=true` (default works for all-in-one). |
| `APP_DB_PORT` | Conditional | `62001` | Required when `APP_DB_ENABLED=true`. |
| `APP_DB_USER` | Conditional | `archivematica` | Required when `APP_DB_ENABLED=true`. |
| `APP_DB_PASSWORD` | Conditional | `demo` | Required when `APP_DB_ENABLED=true`; set in secrets file. |
| `APP_DB_NAME` | Conditional | `MCP` | Required when `APP_DB_ENABLED=true`. |
| `APP_DB_CONN_TIMEOUT_SEC` | Optional | `5` | MCP connection timeout. |
| `APP_DB_QUERY_TIMEOUT_SEC` | Optional | `10` | MCP query timeout. |
| `APP_RUNNING_STUCK_MINUTES` | Optional | `30` | Stalled-running threshold in UI. |

#### App SQLite options

| Variable | Required | Default | Notes |
|---|---|---|---|
| `APP_CUSTOMER_MAP_SQLITE_PATH` | Optional | empty | Enables app-owned SQLite persistence (report templates/mappings) when set, e.g. `/var/lib/am-ops-observer/customer-mappings.db`. |

#### Storage Service DB options (read-only)

| Variable | Required | Default | Notes |
|---|---|---|---|
| `APP_SS_DB_ENABLED` | Optional | `false` | Enables SS package/location enrichment. |
| `APP_SS_DB_HOST` | Conditional | `127.0.0.1` | Required when `APP_SS_DB_ENABLED=true`. |
| `APP_SS_DB_PORT` | Conditional | `62001` | Required when `APP_SS_DB_ENABLED=true`. |
| `APP_SS_DB_USER` | Conditional | `archivematica` | Required when `APP_SS_DB_ENABLED=true`. |
| `APP_SS_DB_PASSWORD` | Conditional | `demo` | Required when `APP_SS_DB_ENABLED=true`; set in secrets file. |
| `APP_SS_DB_NAME` | Conditional | `SS` | Required when `APP_SS_DB_ENABLED=true`. |
| `APP_SS_DB_CONN_TIMEOUT_SEC` | Optional | `5` | SS DB connection timeout. |
| `APP_SS_DB_QUERY_TIMEOUT_SEC` | Optional | `10` | SS DB query timeout. |

#### Prometheus options

| Variable | Required | Default | Notes |
|---|---|---|---|
| `APP_PROM_ENABLED` | Optional | `false` | Enables Prometheus target scraping. |
| `APP_PROM_TARGETS` | Conditional | `http://127.0.0.1:7999/metrics` | Comma-separated target URLs; required in practice when `APP_PROM_ENABLED=true`. |
| `APP_PROM_MATCH_PREFIX` | Optional | `archivematica_` | Metric prefix used for filtering. |
| `APP_PROM_SCRAPE_TIMEOUT_SEC` | Optional | `5` | Per-target scrape timeout. |
| `APP_PROM_SCRAPE_INTERVAL_SEC` | Optional | `15` | Sampling interval for in-memory history. |
| `APP_PROM_HISTORY_MAX_POINTS` | Optional | `720` | Max retained chart points. |

#### Elasticsearch options (read-only)

| Variable | Required | Default | Notes |
|---|---|---|---|
| `APP_ES_ENABLED` | Optional | `false` | Enables AIP lookup and ES-backed details. |
| `APP_ES_ENDPOINT` | Conditional | `http://127.0.0.1:62002` | Required when `APP_ES_ENABLED=true`. |
| `APP_ES_TIMEOUT_SEC` | Optional | `5` | ES HTTP timeout. |
| `APP_ES_LOOKUP_LIMIT` | Optional | `5` | Max ES UUID candidates for transfer lookup. |
| `APP_ES_AIP_INDEX` | Optional | `aipfiles` | ES index used for AIP data. |
| `APP_ES_AIP_PAGE_SIZE` | Optional | `500` | AIP list query page size. |

#### AIP risk threshold options

| Variable | Required | Default | Notes |
|---|---|---|---|
| `APP_RISK_UNKNOWN_HOT_RATE` | Optional | `0.01` | Hot threshold for unknown values ratio. |
| `APP_RISK_UNKNOWN_HOT_ABS` | Optional | `5` | Hot threshold for unknown values absolute count. |
| `APP_RISK_MISSING_IDS_HOT_RATE` | Optional | `0.10` | Hot threshold for missing identifiers ratio. |
| `APP_RISK_MISSING_IDS_HOT_ABS` | Optional | `20` | Hot threshold for missing identifiers absolute count. |
| `APP_RISK_MISSING_CREATED_HOT_RATE` | Optional | `0.10` | Hot threshold for missing created-date ratio. |
| `APP_RISK_MISSING_CREATED_HOT_ABS` | Optional | `20` | Hot threshold for missing created-date absolute count. |
| `APP_RISK_EXT_MISMATCH_HOT_RATE` | Optional | `0.02` | Hot threshold for extension mismatch ratio. |
| `APP_RISK_EXT_MISMATCH_HOT_ABS` | Optional | `10` | Hot threshold for extension mismatch absolute count. |
| `APP_RISK_DUP_FILES_HOT_RATE` | Optional | `0.20` | Hot threshold for duplicate files ratio. |
| `APP_RISK_DUP_FILES_HOT_ABS` | Optional | `50` | Hot threshold for duplicate files absolute count. |
| `APP_RISK_DUP_GROUPS_HOT_ABS` | Optional | `20` | Hot threshold for duplicate groups absolute count. |
| `APP_RISK_INDEX_LAG_P95_HOT_SEC` | Optional | `1800` | Hot threshold for p95 indexing lag (seconds). |
| `APP_RISK_MIN_DIVERSITY_RATIO` | Optional | `0.02` | Minimum acceptable format diversity ratio. |
| `APP_RISK_TINY_FILES_MAX` | Optional | `20` | Max tiny files before warning. |
| `APP_RISK_MIN_UNIQUE_FORMATS` | Optional | `2` | Minimum expected unique formats. |
| `APP_RISK_MIN_UNIQUE_FORMATS_TINY` | Optional | `1` | Minimum unique formats for tiny AIPs. |

## Scope

This project centralizes transfer insights from AM/SS/MySQL/Elasticsearch (and optionally Prometheus) into one API + UI.

Important mode:

- Read-only against Archivematica systems (MCP MySQL, Storage Service DB, Elasticsearch).
- No writes to MCP MySQL.
- No writes to Storage Service DB.
- No writes to Elasticsearch.
- App-owned persistence (report templates and customer mappings) can be stored in local SQLite.

### Troubleshooting module

- Running transfers list with stage, elapsed time, and stuck indicator
- Transfer timeline and error drill-down
- Source connectivity health (AM, SS, MySQL, ES, Prometheus)

### Reporting module

- Monthly transfer totals (success/failed)
- Duration KPIs (avg/p50/p95)
- File totals (total/original/normalized)
- Configurable ad-hoc transfer reports (filters + columns + CSV export)
- Saved report templates in app SQLite

## Current status

Main endpoints:

- `GET /health`
- `GET /ready`
- `GET /metrics` (Prometheus exposition for this app)
- `GET /api/v1/metrics/app` (lightweight app metrics summary used by UI)
- `GET /api/v1/transfers/running?limit=50`
- `GET /api/v1/sips/running?limit=50`
- `GET /api/v1/transfers/completed?limit=50`
- `GET /api/v1/transfers/{transfer_uuid}/summary`
- `GET /api/v1/transfers/{transfer_uuid}/details?limit=100`
- `GET /api/v1/transfers/{transfer_uuid}/timeline?limit=200`
- `GET /api/v1/transfers/{transfer_uuid}/errors?limit=100`
- `GET /api/v1/troubleshooting/stalled?limit=50`
- `GET /api/v1/troubleshooting/hotspots?unit=transfer|sip&hours=24&limit=20`
- `GET /api/v1/troubleshooting/failure-counts?hours=24`
- `GET /api/v1/transfers/failed?hours=24&limit=30`
- `GET /api/v1/troubleshooting/failure-signatures?hours=24&limit=30`
- `GET /api/v1/reports/monthly?customer_id=acme&month=2026-02`
- `GET /api/v1/charts/transfer-durations?customer_id=acme&month=2026-02`
- `POST /api/v1/reports/query` (configurable ad-hoc report run)
- `GET /api/v1/reports/query/options`
- `GET /api/v1/reports/templates`
- `POST /api/v1/reports/templates`
- `GET /api/v1/reports/templates/{id}`
- `DELETE /api/v1/reports/templates/{id}`
- `GET /api/v1/reports/customers?limit=100`
- `GET /api/v1/reports/customer-mappings/{customer_id}`
- `GET /api/v1/metrics/prometheus/live?match=archivematica_`
- `GET /api/v1/charts/prometheus?target=<url>&metric=<name>&minutes=60`
- `GET /api/v1/status/services`
- `GET /api/v1/status/customer-mapping`
- `GET /api/v1/aips?limit=60&cursor=<opaque>`
- `GET /api/v1/aips/{aip_uuid}/stats`
- `GET /api/v1/aips/{aip_uuid}/storage-service`

Current behavior:

- Troubleshooting endpoints are MySQL-backed when `APP_DB_ENABLED=true`.
- Monthly report endpoint is MySQL-backed and returns real KPIs + daily timeseries.
- Customer mapping endpoints are read-only. SQLite mapping backend is used when `APP_CUSTOMER_MAP_SQLITE_PATH` is set.
- Report templates are persisted in app SQLite when `APP_CUSTOMER_MAP_SQLITE_PATH` is set.
- UI includes tabs for Overview, Failed Transfers, Services Status, AIPs, and Configurable Reports.
- If DB is disabled, DB-backed endpoints return `503` with an explicit message.

## Notes on monthly report filtering

- `month` format: `YYYY-MM`
- Special values `all`/`default`/empty disable customer filtering
- Preferred filter mode: SQLite customer mappings (`APP_CUSTOMER_MAP_SQLITE_PATH`)
- Fallback mode (if no mapping backend configured): `Transfers.sourceOfAcquisition = customer_id`
- API returns active mode in `meta.customer_filter_mode`

## Metrics

- `/metrics` exports Prometheus-format app metrics.
- Includes HTTP request counts and durations, in-flight requests, DB query durations and error counters by connector/operation, external probe durations and error counters, report run count/duration, and runtime metrics (uptime, goroutines, memory, GC, CPU, process IO).
- `/api/v1/metrics/app` provides a compact JSON summary used by the Services tab (top slow HTTP endpoints, top slow DB operations, aggregated error counters).

## Run locally (development)

### Requirements

- Go 1.21+

### Start

```bash
go run ./cmd/api
```

### Test

```bash
go test ./...
```

### Try endpoints

```bash
curl -s http://localhost:8080/health | jq
curl -s http://localhost:8080/metrics | head -n 40
curl -s http://localhost:8080/api/v1/metrics/app | jq
```

## Architecture (v1)

- `cmd/api`: app entrypoint
- `internal/config`: runtime config (env-driven)
- `internal/http`: HTTP server and handlers
- `internal/connectors/mysql`: MySQL connector for troubleshooting/report queries
- `internal/realtime` (next): troubleshooting business logic layer
- `internal/reporting` (next): dedicated report aggregation layer

## Packaging and Release

This project includes the same release structure as `csp-web-checker-golang`:

- `.github/workflows/tests.yml` for CI tests
- `.github/workflows/release.yml` for semver-validated releases
- `.github/actions/validate-semver/action.yml` shared semver validation action
- `.goreleaser.yml` for binary + tar.gz + DEB/RPM builds
- `packaging/nfpm/*.sh` package lifecycle scripts
- `packaging/default/am-ops-observer` minimal `/etc/default` bootstrap
- `packaging/default/am-ops-observer-config.env` main non-secret config file
- `packaging/default/am-ops-observer-secrets.env` secrets template file
- `systemd/am-ops-observer.service` systemd unit

## License

- `LICENSE`: GNU Affero General Public License v3 (AGPL-3.0)

## Next milestones

1. Add auth/RBAC and user-scoped report views.
2. Add export endpoints (CSV/PDF) including SS package/location snapshots.
3. Add caching for heavy monthly report aggregations.
