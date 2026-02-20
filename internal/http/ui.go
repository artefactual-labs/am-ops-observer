package http

import nethttp "net/http"

func dashboardHandler(w nethttp.ResponseWriter, r *nethttp.Request) {
	if r.URL.Path != "/" {
		nethttp.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(nethttp.StatusOK)
	_, _ = w.Write([]byte(dashboardHTML))
}

func faviconHandler(w nethttp.ResponseWriter, _ *nethttp.Request) {
	w.WriteHeader(nethttp.StatusNoContent)
}

const dashboardHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>AM Realtime Report UI</title>
  <style>
    @import url("https://fonts.googleapis.com/css?family=Open+Sans:300,400,600,700");

    :root {
      --am-blue: #0e5d8f;
      --am-blue-2: #0971b2;
      --bg: #f7f7f7;
      --paper: #fff;
      --text: #333;
      --muted: #777;
      --line: #ddd;
      --line-soft: #eee;
      --head: #f0f0f0;
      --ok-bg: #dff0d8;
      --ok-text: #3c763d;
      --bad-bg: #f2dede;
      --bad-text: #a94442;
    }

    * { box-sizing: border-box; }

    html { scroll-behavior: smooth; }

    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: "Open Sans", "Helvetica Neue", Helvetica, Arial, sans-serif;
      font-size: 14px;
      line-height: 1.42857143;
    }

    a { color: #428bca; text-decoration: none; }
    a:hover { color: #2a6496; text-decoration: underline; }

    header {
      background: linear-gradient(to right, var(--am-blue) 0, var(--am-blue-2) 100%);
      border-bottom: 1px solid #0b4e79;
      box-shadow: 0 2px 5px rgba(0, 0, 0, 0.15);
    }

    .container {
      margin-right: auto;
      margin-left: auto;
      padding-left: 15px;
      padding-right: 15px;
      width: 100%;
      max-width: 1680px;
    }

    .header-inner {
      min-height: 70px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }

    .navbar-brand {
      color: #fff;
      font-size: 22px;
      font-weight: 300;
      letter-spacing: 0.2px;
    }

    .navbar-brand strong { font-weight: 600; }

    .navbar-note {
      color: rgba(255, 255, 255, 0.88);
      font-size: 13px;
      font-weight: 300;
      text-align: right;
    }

    .sphinx-dev-banner {
      text-align: center;
      background-color: #ffb400;
      padding: 9px 8px 8px;
      border-top: 1px solid #0b4eac;
      box-shadow: inset 1px 1px 1px rgba(125, 125, 125, 0.2);
      font-size: 13px;
      color: #222;
    }

    main { padding: 18px 0 32px; }
    .tabs {
      display: flex;
      gap: 8px;
      margin-bottom: 14px;
      border-bottom: 1px solid var(--line);
      padding-bottom: 8px;
    }

    .tab-btn {
      border: 1px solid #c7d7e5;
      background: #f3f8fc;
      color: #0e5d8f;
      padding: 6px 10px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
    }

    .tab-btn.active {
      background: #0e5d8f;
      color: #fff;
      border-color: #0e5d8f;
    }

    .tab-pane { display: none; }
    .tab-pane.active { display: block; }

    .row {
      display: flex;
      flex-wrap: wrap;
      margin-left: -15px;
      margin-right: -15px;
    }

    .col-main,
    .col-side {
      position: relative;
      min-height: 1px;
      padding-left: 15px;
      padding-right: 15px;
      width: 100%;
    }

    .sphinx-body,
    .sphinx-sidebar {
      background: var(--paper);
      border: 1px solid var(--line);
      box-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
      padding: 16px;
      margin-bottom: 16px;
    }

    h1 {
      margin: 0 0 12px;
      font-size: 30px;
      font-weight: 300;
      border-bottom: 1px solid var(--line-soft);
      padding-bottom: 8px;
      color: #444;
    }

    h2 {
      margin: 20px 0 10px;
      font-size: 22px;
      font-weight: 400;
      color: #444;
      border-bottom: 1px solid var(--line-soft);
      padding-bottom: 6px;
      scroll-margin-top: 84px;
    }

    h3 {
      margin: 0;
      font-size: 16px;
      font-weight: 600;
      color: #444;
    }

    .admonition {
      font-size: 0.9em;
      margin: 1em 0;
      border: 1px solid var(--line-soft);
      background-color: #f7f7f7;
      box-shadow: 0 8px 6px -8px #93a1a1;
    }

    .admonition-title {
      margin: 0;
      padding: 0.25em 0.6em;
      color: #fff;
      border-bottom: 1px solid #eee8d5;
      background-color: #cb4b16;
      font-weight: 600;
    }

    .admonition p {
      margin: 0.6em 1em;
      color: #444;
    }

    .summary-panel {
      border: 1px solid var(--line);
      background: var(--paper);
      margin-bottom: 14px;
    }
    .summary-panel .panel-heading { border-bottom: 1px solid var(--line); }
    .summary-panel .panel-body { padding: 10px 12px 12px; }

    .panel-grid {
      display: grid;
      gap: 14px;
      grid-template-columns: 1.2fr 1fr;
      margin-bottom: 14px;
    }

    .panel {
      border: 1px solid var(--line);
      background: var(--paper);
    }

    .panel-heading {
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      background: var(--head);
    }

    .panel-body { padding: 10px 12px 12px; }

    table {
      width: 100%;
      max-width: 100%;
      border-collapse: collapse;
    }

    th,
    td {
      padding: 8px;
      line-height: 1.42857143;
      vertical-align: top;
      border-top: 1px solid var(--line);
      text-align: left;
      font-size: 13px;
    }

    thead th {
      vertical-align: bottom;
      border-bottom: 2px solid var(--line);
      border-top: 0;
      color: #555;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      background: #fafafa;
    }

    tbody tr:nth-child(odd) td { background: #f9f9f9; }

    .row-click { cursor: pointer; }
    .row-click:hover td { background: #f5f5f5 !important; }
    .row-failed td {
      background: #fdf2f2 !important;
      color: #8f2c2a;
    }
    .row-recovered td {
      background: #fff7e6 !important;
      color: #8a5a14;
    }
    .row-selected td {
      outline: 2px solid #0e5d8f;
      outline-offset: -2px;
    }

    .pill {
      display: inline-block;
      border-radius: 2px;
      font-size: 11px;
      padding: 2px 6px;
      font-weight: 700;
      border: 1px solid transparent;
      text-transform: uppercase;
      letter-spacing: 0.2px;
    }

    .ok {
      color: var(--ok-text);
      background: var(--ok-bg);
      border-color: #d6e9c6;
    }

    .bad {
      color: var(--bad-text);
      background: var(--bad-bg);
      border-color: #ebccd1;
    }
    .warn {
      color: #8a6d3b;
      background: #fcf8e3;
      border-color: #faebcc;
    }
    .info {
      color: #31708f;
      background: #d9edf7;
      border-color: #bce8f1;
    }

    .mono {
      font-family: Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      word-break: break-all;
    }

    canvas {
      width: 100%;
      height: 220px;
      border: 1px solid var(--line);
      background: #fff;
    }

    .hint {
      margin-top: 8px;
      color: var(--muted);
      font-size: 12px;
    }

    .detail-summary {
      margin-bottom: 10px;
      border: 1px solid var(--line);
      background: #f9f9f9;
      padding: 8px;
      font-size: 12px;
    }

    .detail-summary table { margin: 0; }
    .detail-summary th,
    .detail-summary td {
      padding: 5px 6px;
      border-top: 1px solid #e7e7e7;
      font-size: 12px;
      background: transparent;
    }

    .detail-summary th {
      width: 42%;
      text-transform: none;
      letter-spacing: 0;
      font-weight: 600;
      color: #444;
      border-bottom: 0;
      background: transparent;
    }

    .detail-summary td { color: #333; }

    .perf-table-wrap { margin-bottom: 10px; }

    .perf-table td,
    .perf-table th {
      font-size: 12px;
      padding: 6px;
    }

    .perf-table td:nth-child(2) {
      max-width: 260px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .service-kpis {
      display: grid;
      gap: 12px;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      margin: 10px 0 14px;
    }

    .service-table td,
    .service-table th {
      font-size: 12px;
    }

    .metric-ok { color: #3c763d; font-weight: 600; }
    .metric-warn { color: #8a6d3b; font-weight: 600; }
    .metric-hot { color: #a94442; font-weight: 700; }

    pre {
      margin: 0;
      padding: 10px;
      border: 1px solid var(--line);
      background: #fafafa;
      max-height: 340px;
      overflow: auto;
      font: 12px/1.35 Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    }

    .toc-title {
      margin-top: 0;
      margin-bottom: 8px;
      font-size: 16px;
      font-weight: 600;
      color: #444;
      border-bottom: 1px solid var(--line-soft);
      padding-bottom: 6px;
    }

    .toc-tree {
      list-style: none;
      padding-left: 0;
      margin: 0;
    }

    .toc-tree li { margin-bottom: 7px; }

    .latest-update {
      margin-top: 10px;
      color: #777;
      font-size: 12px;
    }

    @media (min-width: 768px) {
      .col-main { width: 100%; }
      .col-side { width: 100%; }
    }

    @media (max-width: 1024px) {
      .kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .panel-grid { grid-template-columns: 1fr; }
      .service-kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }

    @media (max-width: 640px) {
      .header-inner {
        flex-direction: column;
        align-items: flex-start;
        justify-content: center;
        padding: 10px 0;
      }
      .navbar-note { text-align: left; }
      .kpi-grid { grid-template-columns: 1fr; }
      .service-kpis { grid-template-columns: 1fr; }
      h1 { font-size: 26px; }
      h2 { font-size: 20px; }
    }
  </style>
</head>
<body>
  <header>
    <div class="container header-inner">
      <div class="navbar-brand"><strong>Archivematica</strong> Transfer Operations</div>
      <div class="navbar-note">Realtime troubleshooting and monthly reporting</div>
    </div>
  </header>

  <div class="sphinx-dev-banner">Live dashboard for AM/SS/MCP data sources. Click completed transfers to inspect DB and Elasticsearch details.</div>

  <main>
    <div class="container">
      <div class="row">
        <div class="col-main">
          <div class="sphinx-body">
            <div class="tabs">
              <button class="tab-btn active" id="tab-btn-overview" data-tab="overview">Overview</button>
              <button class="tab-btn" id="tab-btn-failed" data-tab="failed">Failed Transfers</button>
              <button class="tab-btn" id="tab-btn-services" data-tab="services">Services Status</button>
              <button class="tab-btn" id="tab-btn-aips" data-tab="aips">AIPs (ES)</button>
              <button class="tab-btn" id="tab-btn-reports" data-tab="reports">Reports</button>
            </div>

            <section id="tab-overview" class="tab-pane active">
            <h1 id="transfer-monitoring">Transfer Monitoring</h1>

            <div class="admonition">
              <p class="admonition-title">Important</p>
              <p>Metrics and troubleshooting panels combine MySQL (MCP), Prometheus endpoints, and Elasticsearch lookup results.</p>
            </div>

            <article class="summary-panel">
              <div class="panel-heading"><h3>Workflow Summary</h3></div>
              <div class="panel-body">
                <div style="margin-bottom:8px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
                  <label for="failure-window">Failures window</label>
                  <select id="failure-window">
                    <option value="24">Last 24h</option>
                    <option value="168">Last 7 days</option>
                    <option value="720">Last month</option>
                    <option value="8760">Last year</option>
                    <option value="all">Forever</option>
                  </select>
                </div>
                <table class="service-table">
                  <thead><tr><th>Type</th><th>Running</th><th>Stalled</th><th id="workflow-failure-col">Failures (24h)</th></tr></thead>
                  <tbody id="workflow-summary-body"><tr><td colspan="4">Loading...</td></tr></tbody>
                </table>
              </div>
            </article>

            <h2 id="troubleshooting">Troubleshooting</h2>
            <section class="panel-grid">
              <article class="panel">
                <div class="panel-heading"><h3>Running Transfers and SIPs</h3></div>
                <div class="panel-body">
                  <table>
                    <thead><tr><th>Type</th><th>Name/UUID</th><th>Stage</th><th>Status</th><th>Elapsed</th></tr></thead>
                    <tbody id="running-body"><tr><td colspan="5">Loading...</td></tr></tbody>
                  </table>
                </div>
              </article>
            </section>
            <section class="panel-grid">
              <article class="panel">
                <div class="panel-heading"><h3>Error Hotspots (24h)</h3></div>
                <div class="panel-body">
                  <table>
                    <thead><tr><th>Microservice</th><th>Failures</th><th>Transfers</th></tr></thead>
                    <tbody id="hotspot-body"><tr><td colspan="3">Loading...</td></tr></tbody>
                  </table>
                </div>
              </article>
            </section>

            <h2 id="charts">Charts</h2>
            <section class="panel-grid">
              <article class="panel">
                <div class="panel-heading"><h3>Monthly Transfer Durations (avg/p95 sec)</h3></div>
                <div class="panel-body">
                  <canvas id="duration-chart" width="560" height="220"></canvas>
                  <div class="hint">Source: <span class="mono">/api/v1/charts/transfer-durations</span></div>
                </div>
              </article>
              <article class="panel">
                <div class="panel-heading"><h3>Prometheus Metric (mcpclient_job_total)</h3></div>
                <div class="panel-body">
                  <canvas id="prom-chart" width="560" height="220"></canvas>
                  <div class="hint">Source: <span class="mono">/api/v1/charts/prometheus</span></div>
                </div>
              </article>
            </section>

            <h2 id="completed-transfers">Completed Transfers</h2>
            <section class="panel-grid">
              <article class="panel">
                <div class="panel-heading"><h3>Completed Transfers (click row for details)</h3></div>
                <div class="panel-body">
                  <table>
                    <thead><tr><th>Transfer</th><th>Status</th><th>Completed</th><th>Duration</th><th>Files</th></tr></thead>
                    <tbody id="completed-body"><tr><td colspan="5">Loading...</td></tr></tbody>
                  </table>
                </div>
              </article>
              <article class="panel" id="transfer-details">
                <div class="panel-heading"><h3>Selected Transfer Details (DB + ES + SS)</h3></div>
                <div class="panel-body">
                  <div class="detail-summary">
                    <table>
                      <tbody>
                        <tr><th>Transfer UUID</th><td id="ds-uuid">-</td></tr>
                        <tr><th>Name</th><td id="ds-name">-</td></tr>
                        <tr><th>Status</th><td id="ds-status">-</td></tr>
                        <tr><th>Duration</th><td id="ds-duration">-</td></tr>
                        <tr><th>Files</th><td id="ds-files">-</td></tr>
                        <tr><th>Failed Jobs</th><td id="ds-failed-jobs">-</td></tr>
                        <tr><th>Tasks</th><td id="ds-tasks">-</td></tr>
                        <tr><th>Transfer Size</th><td id="ds-size">-</td></tr>
                        <tr><th>Timeline Entries</th><td id="ds-timeline">-</td></tr>
                        <tr><th>Error Entries</th><td id="ds-errors">-</td></tr>
                        <tr><th>Longest Microservice</th><td id="ds-bottleneck">-</td></tr>
                        <tr><th>SS Packages</th><td id="ds-ss-packages">-</td></tr>
                        <tr><th>ES Hits</th><td id="ds-es">-</td></tr>
                      </tbody>
                    </table>
                  </div>
                  <div class="perf-table-wrap">
                    <table class="perf-table">
                      <thead><tr><th>SS UUID</th><th>Type</th><th>Status</th><th>Size</th><th>Location</th><th>Path</th><th>Files</th></tr></thead>
                      <tbody id="sspkg-body"><tr><td colspan="7">Click a completed transfer to load Storage Service package details.</td></tr></tbody>
                    </table>
                  </div>
                  <div class="perf-table-wrap">
                    <table class="perf-table">
                      <thead><tr><th>Phase</th><th>Microservice</th><th>CPU</th><th>Duration</th><th>Tasks</th><th>CPU/Wall</th><th>Hint</th></tr></thead>
                      <tbody id="perf-body"><tr><td colspan="7">Click a completed transfer to load microservice performance.</td></tr></tbody>
                    </table>
                  </div>
                  <pre id="details-json">Click a completed transfer row to load summary, timeline, errors and Elasticsearch hits.</pre>
                </div>
              </article>
            </section>
            </section>

            <section id="tab-failed" class="tab-pane">
              <h1 id="failed-transfers">Failed Transfers</h1>

              <div class="admonition">
                <p class="admonition-title">Important</p>
                <p>Triage view for failed transfers, recurring signatures, and quick drilldown into transfer details.</p>
              </div>

              <section class="panel-grid">
                <article class="panel">
                  <div class="panel-heading">
                    <h3 id="failed-recent-title">Recent Failed Transfers</h3>
                    <label style="display:flex;align-items:center;gap:6px">
                      Window
                      <select id="failure-window-failed">
                        <option value="24">Last 24h</option>
                        <option value="168">Last 7 days</option>
                        <option value="720">Last month</option>
                        <option value="8760">Last year</option>
                        <option value="all">Forever</option>
                      </select>
                    </label>
                  </div>
                  <div class="panel-body">
                    <table class="service-table">
                      <thead><tr><th>Transfer</th><th>Status</th><th>Microservice</th><th>Failed At</th><th>Duration</th><th>Files</th><th>Failed Jobs</th><th>Raw</th></tr></thead>
                      <tbody id="failed-body"><tr><td colspan="8">Loading...</td></tr></tbody>
                    </table>
                  </div>
                </article>
              </section>

              <article class="panel">
                <div class="panel-heading"><h3>Selected Failed Transfer Details</h3></div>
                <div class="panel-body">
                  <div class="hint" id="failed-selected-info">No failed transfer selected.</div>
                  <table class="service-table">
                    <thead><tr><th>Ended</th><th>Microservice</th><th>Job</th><th>File</th><th>Exit</th><th>Task Output</th></tr></thead>
                    <tbody id="failed-task-body"><tr><td colspan="6">Click a failed transfer row to load failed task output.</td></tr></tbody>
                  </table>
                  <pre id="failed-details-json">Click a failed transfer row to load details.</pre>
                </div>
              </article>

              <article class="panel">
                <div class="panel-heading"><h3 id="failed-signatures-title">Global Failure Signatures</h3></div>
                <div class="panel-body">
                  <table class="service-table">
                    <thead><tr><th>Microservice</th><th>Signature</th><th>Failures</th><th>Transfers</th><th>Last Seen</th></tr></thead>
                    <tbody id="failed-signature-body"><tr><td colspan="5">Loading...</td></tr></tbody>
                  </table>
                </div>
              </article>
            </section>

            <section id="tab-services" class="tab-pane">
              <h1 id="services-status">Services Status</h1>

              <div class="admonition">
                <p class="admonition-title">Important</p>
                <p>Service probes include connectivity, uptime and main operational counters for MySQL, Elasticsearch and AM service metrics endpoints.</p>
              </div>

              <div class="service-kpis">
                <article class="kpi"><div class="kpi-label">MySQL</div><div class="kpi-value" id="svc-mysql">-</div></article>
                <article class="kpi"><div class="kpi-label">Elasticsearch</div><div class="kpi-value" id="svc-es">-</div></article>
                <article class="kpi"><div class="kpi-label">Prom Targets Up</div><div class="kpi-value" id="svc-prom-up">-</div></article>
                <article class="kpi"><div class="kpi-label">Last Check</div><div class="kpi-value" id="svc-updated">-</div></article>
              </div>

              <section class="panel-grid">
                <article class="panel">
                  <div class="panel-heading"><h3>MySQL, SS DB & Elasticsearch</h3></div>
                  <div class="panel-body">
                    <table class="service-table">
                      <thead><tr><th>Service</th><th>Status</th><th>Ping</th><th>Uptime</th><th>Main Stats</th></tr></thead>
                      <tbody id="services-core-body"><tr><td colspan="5">Loading...</td></tr></tbody>
                    </table>
                  </div>
                </article>

                <article class="panel">
                  <div class="panel-heading"><h3>AM Prometheus Targets</h3></div>
                  <div class="panel-body">
                    <table class="service-table">
                      <thead><tr><th>Target</th><th>Status</th><th>Ping</th><th>Uptime</th><th>CPU</th><th>Memory</th><th>Goroutines</th><th>Samples</th></tr></thead>
                      <tbody id="services-prom-body"><tr><td colspan="8">Loading...</td></tr></tbody>
                    </table>
                  </div>
                </article>
              </section>
              <section class="panel-grid">
                <article class="panel">
                  <div class="panel-heading"><h3>App Metrics: Slow Endpoints</h3></div>
                  <div class="panel-body">
                    <table class="service-table">
                      <thead><tr><th>Method</th><th>Path</th><th>Status</th><th>Count</th><th>Avg ms</th></tr></thead>
                      <tbody id="services-app-http-body"><tr><td colspan="5">Loading...</td></tr></tbody>
                    </table>
                  </div>
                </article>
                <article class="panel">
                  <div class="panel-heading"><h3>App Metrics: Slow DB Ops</h3></div>
                  <div class="panel-body">
                    <table class="service-table">
                      <thead><tr><th>Connector</th><th>Operation</th><th>Count</th><th>Errors</th><th>Avg ms</th></tr></thead>
                      <tbody id="services-app-db-body"><tr><td colspan="5">Loading...</td></tr></tbody>
                    </table>
                    <div class="hint" id="services-app-errors">Errors: -</div>
                  </div>
                </article>
              </section>
            </section>

            <section id="tab-aips" class="tab-pane">
              <h1 id="aips-es">AIPs from Elasticsearch</h1>

              <div class="admonition">
                <p class="admonition-title">Important</p>
                <p>Browse AIPs from index <span class="mono">aipfiles</span>. Select one AIP to inspect file counts, extension/format distribution, and normalized-object statistics.</p>
              </div>

              <section class="panel-grid">
                <article class="panel">
                  <div class="panel-heading"><h3>AIP List</h3></div>
                  <div class="panel-body">
                    <table class="service-table">
                      <thead><tr><th>AIP UUID</th><th>SIP Name</th></tr></thead>
                      <tbody id="aip-list-body"><tr><td colspan="2">Loading...</td></tr></tbody>
                    </table>
                    <div style="margin-top:8px">
                      <button class="tab-btn" id="aip-load-more" type="button">Load More</button>
                    </div>
                  </div>
                </article>

                <article class="panel">
                  <div class="panel-heading"><h3>Selected AIP Stats</h3></div>
                  <div class="panel-body">
                    <div class="detail-summary">
                      <table>
                        <tbody>
                          <tr><th>AIP UUID</th><td id="aip-d-uuid">-</td></tr>
                          <tr><th>SIP Names</th><td id="aip-d-sips">-</td></tr>
                          <tr><th>Files Total</th><td id="aip-d-files">-</td></tr>
                          <tr><th>Unique File UUIDs</th><td id="aip-d-file-uuids">-</td></tr>
                          <tr><th>Total Size</th><td id="aip-d-size">-</td></tr>
                          <tr><th>Avg File Size</th><td id="aip-d-avg-size">-</td></tr>
                          <tr><th>Created-By-App Range</th><td id="aip-d-created-range">-</td></tr>
                          <tr><th>Indexed Range</th><td id="aip-d-indexed-range">-</td></tr>
                          <tr><th>Event Date Range</th><td id="aip-d-event-range">-</td></tr>
                          <tr><th>With Normalized</th><td id="aip-d-with-norm">-</td></tr>
                          <tr><th>Without Normalized</th><td id="aip-d-without-norm">-</td></tr>
                          <tr><th>Normalized Refs Total</th><td id="aip-d-norm-total">-</td></tr>
                          <tr><th>Normalized/Object Avg</th><td id="aip-d-norm-avg">-</td></tr>
                          <tr><th>Status Mix</th><td id="aip-d-status-mix">-</td></tr>
                          <tr><th>Origin Mix</th><td id="aip-d-origin-mix">-</td></tr>
                          <tr><th>Accession IDs</th><td id="aip-d-accession-mix">-</td></tr>
                          <tr><th>Is Part Of</th><td id="aip-d-ispartof-mix">-</td></tr>
                          <tr><th>AM Version Mix</th><td id="aip-d-am-version-mix">-</td></tr>
                          <tr><th>PREMIS Event Types</th><td id="aip-d-premis-type-mix">-</td></tr>
                          <tr><th>PREMIS Outcomes</th><td id="aip-d-premis-outcome-mix">-</td></tr>
                          <tr><th>PREMIS Tools</th><td id="aip-d-premis-tool-mix">-</td></tr>
                          <tr><th>Unknown Formats</th><td id="aip-d-unknown-formats">-</td></tr>
                          <tr><th>Missing Identifiers</th><td id="aip-d-missing-identifiers">-</td></tr>
                          <tr><th>Missing Created Date</th><td id="aip-d-missing-created">-</td></tr>
                          <tr><th>Ext/Format Mismatch</th><td id="aip-d-ext-mismatch">-</td></tr>
                          <tr><th>Duplicate Filenames</th><td id="aip-d-dup-filenames">-</td></tr>
                          <tr><th>Unique Format Signatures</th><td id="aip-d-unique-formats">-</td></tr>
                          <tr><th>Format Diversity Ratio</th><td id="aip-d-format-diversity">-</td></tr>
                          <tr><th>Index Lag</th><td id="aip-d-index-lag">-</td></tr>
                        </tbody>
                      </table>
                    </div>
                    <div class="perf-table-wrap">
                      <table class="perf-table">
                        <thead><tr><th>Extension</th><th>Count</th></tr></thead>
                        <tbody id="aip-ext-body"><tr><td colspan="2">Select an AIP to load extension/format stats.</td></tr></tbody>
                      </table>
                    </div>
                    <div class="perf-table-wrap">
                      <table class="perf-table">
                        <thead><tr><th>PRONOM/Registry Key</th><th>Count</th></tr></thead>
                        <tbody id="aip-pronom-body"><tr><td colspan="2">Select an AIP to load format registry stats.</td></tr></tbody>
                      </table>
                    </div>
                    <div class="perf-table-wrap">
                      <table class="perf-table">
                        <thead><tr><th>Registry Key</th><th>Format Name</th><th>Version</th><th>Count</th></tr></thead>
                        <tbody id="aip-format-version-body"><tr><td colspan="4">Select an AIP to load format versions.</td></tr></tbody>
                      </table>
                      <div style="margin-top:8px">
                        <button class="tab-btn" id="aip-export-format-versions" type="button">Export Format Versions CSV</button>
                      </div>
                    </div>
                    <div class="perf-table-wrap">
                      <div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:8px">
                        <label for="aip-largest-status-filter">Status</label>
                        <select id="aip-largest-status-filter">
                          <option value="">All</option>
                        </select>
                        <label for="aip-largest-ext-filter">Ext</label>
                        <input id="aip-largest-ext-filter" type="text" placeholder="e.g. tif" style="min-width:100px" />
                        <label for="aip-largest-sort">Sort</label>
                        <select id="aip-largest-sort">
                          <option value="size_desc">Size desc</option>
                          <option value="size_asc">Size asc</option>
                          <option value="path_asc">Path A-Z</option>
                          <option value="path_desc">Path Z-A</option>
                        </select>
                      </div>
                      <table class="perf-table">
                        <thead><tr><th>Largest File</th><th>Size</th><th>Format</th><th>Status</th><th>Created</th><th>Indexed</th></tr></thead>
                        <tbody id="aip-largest-body"><tr><td colspan="6">Select an AIP to load largest files.</td></tr></tbody>
                      </table>
                      <div style="margin-top:8px">
                        <button class="tab-btn" id="aip-export-largest-files" type="button">Export Largest Files CSV</button>
                      </div>
                    </div>
                    <div class="perf-table-wrap">
                      <table class="perf-table">
                        <thead><tr><th>SS UUID</th><th>Type</th><th>Status</th><th>Size</th><th>Stored</th><th>Location</th><th>Path</th><th>Pipeline</th><th>Files</th></tr></thead>
                        <tbody id="aip-ss-body"><tr><td colspan="9">Select an AIP to load Storage Service package details.</td></tr></tbody>
                      </table>
                    </div>
                  </div>
                </article>
              </section>
            </section>

            <section id="tab-reports" class="tab-pane">
              <h1 id="configurable-reports">Configurable Reports</h1>

              <div class="admonition">
                <p class="admonition-title">Important</p>
                <p>Ad-hoc transfer report builder with selectable date range, status filter, columns, and CSV export.</p>
              </div>

              <article class="panel">
                <div class="panel-heading"><h3>Report Builder</h3></div>
                <div class="panel-body">
                  <div style="display:flex;flex-wrap:wrap;gap:10px;align-items:end">
                    <label>Date From<br><input id="report-date-from" type="date" /></label>
                    <label>Date To<br><input id="report-date-to" type="date" /></label>
                    <label>Status<br>
                      <select id="report-status">
                        <option value="all">all</option>
                        <option value="success">success</option>
                        <option value="failed">failed</option>
                        <option value="completed_with_non_blocking_errors">completed_with_non_blocking_errors</option>
                      </select>
                    </label>
                    <label>Customer ID<br><input id="report-customer-id" type="text" placeholder="all / customer id" /></label>
                    <label>Limit<br><input id="report-limit" type="number" min="1" max="1000" value="100" style="width:90px" /></label>
                    <label>Offset<br><input id="report-offset" type="number" min="0" value="0" style="width:90px" /></label>
                    <button class="tab-btn" id="report-run" type="button">Run Report</button>
                    <button class="tab-btn" id="report-export-csv" type="button">Export CSV</button>
                    <input id="report-template-name" type="text" placeholder="template name" style="min-width:180px" />
                    <input id="report-template-description" type="text" placeholder="description (optional)" style="min-width:220px" />
                    <button class="tab-btn" id="report-save-template" type="button">Save Template</button>
                  </div>
                  <div style="display:flex;flex-wrap:wrap;gap:8px;align-items:end;margin-top:8px">
                    <label>Templates<br>
                      <select id="report-template-select" style="min-width:280px">
                        <option value="">Select template...</option>
                      </select>
                    </label>
                    <button class="tab-btn" id="report-load-template" type="button">Load Template</button>
                    <button class="tab-btn" id="report-delete-template" type="button">Delete Template</button>
                    <span id="report-template-flash" class="hint" style="display:none;font-weight:600"></span>
                  </div>
                  <div style="margin-top:10px">
                    <div class="hint">Columns</div>
                    <div id="report-columns" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:6px"></div>
                  </div>
                  <div class="hint" id="report-summary" style="margin-top:8px">Run a report to see results.</div>
                  <table class="service-table">
                    <thead id="report-results-head"><tr><th>Results</th></tr></thead>
                    <tbody id="report-results-body"><tr><td>Run a report to load rows.</td></tr></tbody>
                  </table>
                  <pre id="report-debug-json">No report run yet.</pre>
                </div>
              </article>
            </section>
          </div>
        </div>

        <aside class="col-side">
          <div class="sphinx-sidebar">
            <h3 class="toc-title">In This Page</h3>
            <ul class="toc-tree">
              <li><a href="#transfer-monitoring">Transfer Monitoring</a></li>
              <li><a href="#troubleshooting">Troubleshooting</a></li>
              <li><a href="#charts">Charts</a></li>
              <li><a href="#completed-transfers">Completed Transfers</a></li>
              <li><a href="#failed-transfers">Failed Transfers</a></li>
              <li><a href="#transfer-details">Selected Transfer Details</a></li>
              <li><a href="#services-status">Services Status</a></li>
              <li><a href="#aips-es">AIPs (Elasticsearch)</a></li>
              <li><a href="#configurable-reports">Configurable Reports</a></li>
            </ul>
            <div class="latest-update">Auto-refresh interval: 15 seconds</div>
          </div>

          <div class="sphinx-sidebar">
            <h3 class="toc-title">Data Endpoints</h3>
            <ul class="toc-tree">
              <li><span class="mono">/api/v1/transfers/running</span></li>
              <li><span class="mono">/api/v1/sips/running</span></li>
              <li><span class="mono">/api/v1/troubleshooting/stalled</span></li>
              <li><span class="mono">/api/v1/troubleshooting/hotspots</span></li>
              <li><span class="mono">/api/v1/transfers/completed</span></li>
              <li><span class="mono">/api/v1/transfers/failed</span></li>
              <li><span class="mono">/api/v1/troubleshooting/failure-signatures</span></li>
              <li><span class="mono">/api/v1/transfers/{uuid}/details</span></li>
              <li><span class="mono">/api/v1/charts/transfer-durations</span></li>
              <li><span class="mono">/api/v1/charts/prometheus</span></li>
              <li><span class="mono">/api/v1/status/services</span></li>
              <li><span class="mono">/api/v1/status/customer-mapping</span></li>
              <li><span class="mono">/api/v1/metrics/app</span></li>
              <li><span class="mono">/api/v1/settings/risk-thresholds</span></li>
              <li><span class="mono">/api/v1/aips</span></li>
              <li><span class="mono">/api/v1/aips/{aip_uuid}/stats</span></li>
              <li><span class="mono">/api/v1/aips/{aip_uuid}/storage-service</span></li>
              <li><span class="mono">/metrics</span></li>
              <li><span class="mono">/api/v1/reports/query/options</span></li>
              <li><span class="mono">/api/v1/reports/query</span></li>
              <li><span class="mono">/api/v1/reports/templates</span></li>
            </ul>
          </div>
        </aside>
      </div>
    </div>
  </main>

  <script>
    const text = (id, v) => document.getElementById(id).textContent = v;
    const html = (id, v) => document.getElementById(id).innerHTML = v;
    const q = (s) => document.querySelector(s);
    const qq = (s) => Array.from(document.querySelectorAll(s));

    async function getJSON(url) {
      const r = await fetch(url);
      if (!r.ok) throw new Error(url + " -> " + r.status);
      return r.json();
    }

    function fmtSec(sec) {
      if (!Number.isFinite(sec)) return "-";
      if (sec < 60) return sec + "s";
      const m = Math.floor(sec / 60);
      const s = sec % 60;
      return m + "m " + s + "s";
    }

    function fmtDuration(sec) {
      if (!Number.isFinite(sec) || sec <= 0) return "-";
      const d = Math.floor(sec / 86400);
      const h = Math.floor((sec % 86400) / 3600);
      const m = Math.floor((sec % 3600) / 60);
      if (d > 0) return d + "d " + h + "h";
      if (h > 0) return h + "h " + m + "m";
      return m + "m";
    }

    function statusPill(ok) {
      return '<span class="pill ' + (ok ? 'ok' : 'bad') + '">' + (ok ? 'ok' : 'down') + '</span>';
    }

    function classifyMetric(v, warn, hot) {
      if (!Number.isFinite(v)) return 'metric-ok';
      if (v >= hot) return 'metric-hot';
      if (v >= warn) return 'metric-warn';
      return 'metric-ok';
    }

    function riskBadge(ok, label) {
      return '<span class="pill ' + (ok ? 'ok' : 'bad') + '">' + label + '</span>';
    }

    function riskCountBadge(count) {
      const n = Number(count || 0);
      return riskBadge(n === 0, String(n));
    }

    function isRiskByRate(count, total, hotRate, hotAbs) {
      const n = Number(count || 0);
      const t = Math.max(1, Number(total || 0));
      const rate = n / t;
      return n >= Number(hotAbs || 0) || rate >= Number(hotRate || 0);
    }

    function switchTab(tab) {
      qq('.tab-btn[data-tab]').forEach((b) => b.classList.toggle('active', b.dataset.tab === tab));
      q('#tab-overview').classList.toggle('active', tab === 'overview');
      q('#tab-failed').classList.toggle('active', tab === 'failed');
      q('#tab-services').classList.toggle('active', tab === 'services');
      q('#tab-aips').classList.toggle('active', tab === 'aips');
      q('#tab-reports').classList.toggle('active', tab === 'reports');
      if (tab === 'failed') {
        loadFailedTab();
      }
      if (tab === 'services') {
        loadServicesStatus();
      }
      if (tab === 'aips') {
        loadAIPList(true);
      }
      if (tab === 'reports') {
        loadReportOptions().then(() => loadReportTemplates()).then(() => runReportQuery());
      }
    }

    let aipCursor = '';
    let aipCurrentStats = null;
    let failureWindow = '24';
    let reportColumns = [];
    let reportSelectedColumns = [];
    let reportRows = [];
    let reportTemplates = [];
    let riskThresholds = {
      unknown_hot_rate: 0.01,
      unknown_hot_abs: 5,
      missing_ids_hot_rate: 0.10,
      missing_ids_hot_abs: 20,
      missing_created_hot_rate: 0.10,
      missing_created_hot_abs: 20,
      ext_mismatch_hot_rate: 0.02,
      ext_mismatch_hot_abs: 10,
      dup_files_hot_rate: 0.20,
      dup_files_hot_abs: 50,
      dup_groups_hot_abs: 20,
      index_lag_p95_hot_sec: 1800,
      min_diversity_ratio: 0.02,
      tiny_files_max: 20,
      min_unique_formats: 2,
      min_unique_formats_for_tiny: 1,
    };

    function failureWindowLabel() {
      switch (failureWindow) {
      case '24': return '24h';
      case '168': return '7d';
      case '720': return '30d';
      case '8760': return '1y';
      default: return 'all';
      }
    }

    function selectedReportColumns() {
      return qq('input[name="report-column"]:checked').map((el) => el.value);
    }

    function renderReportColumns(columns) {
      const container = q('#report-columns');
      container.innerHTML = '';
      (columns || []).forEach((c, idx) => {
        const id = 'report-col-' + idx;
        const label = document.createElement('label');
        label.setAttribute('for', id);
        label.style.display = 'flex';
        label.style.alignItems = 'center';
        label.style.gap = '6px';
        label.innerHTML =
          '<input id="' + id + '" type="checkbox" name="report-column" value="' + c.key + '"' +
          (reportSelectedColumns.includes(c.key) ? ' checked' : '') + ' />' +
          '<span>' + c.label + ' <span class="mono">(' + c.key + ')</span></span>';
        container.appendChild(label);
      });
    }

    async function loadReportOptions() {
      if (reportColumns.length) return;
      const res = await getJSON('/api/v1/reports/query/options');
      reportColumns = res?.data?.columns || [];
      const defaults = ['transfer_uuid', 'name', 'status', 'completed_at', 'duration_seconds', 'files_total', 'failed_tasks'];
      reportSelectedColumns = defaults.filter((k) => reportColumns.some((c) => c.key === k));
      renderReportColumns(reportColumns);
      await loadReportTemplates();
    }

    function reportColumnLabel(key) {
      const found = reportColumns.find((c) => c.key === key);
      return found ? found.label : key;
    }

    function renderReportTemplateOptions() {
      const sel = q('#report-template-select');
      if (!sel) return;
      sel.innerHTML = '<option value="">Select template...</option>';
      (reportTemplates || []).forEach((t) => {
        const opt = document.createElement('option');
        opt.value = String(t.id);
        opt.textContent = t.name + (t.description ? (' - ' + t.description) : '');
        sel.appendChild(opt);
      });
    }

    async function loadReportTemplates() {
      try {
        const res = await getJSON('/api/v1/reports/templates?limit=200');
        reportTemplates = res?.data || [];
        renderReportTemplateOptions();
      } catch (err) {
        reportTemplates = [];
        renderReportTemplateOptions();
      }
    }

    function flashReportTemplate(icon, msg, ok) {
      const el = q('#report-template-flash');
      if (!el) return;
      el.textContent = icon + ' ' + msg;
      el.style.display = 'inline-block';
      el.style.color = ok ? '#3c763d' : '#a94442';
      clearTimeout(flashReportTemplate._timer);
      flashReportTemplate._timer = setTimeout(() => {
        el.style.display = 'none';
      }, 2000);
    }

    function renderReportResults(columns, rows) {
      const thead = q('#report-results-head');
      const tbody = q('#report-results-body');
      thead.innerHTML = '<tr>' + columns.map((c) => '<th>' + reportColumnLabel(c) + '</th>').join('') + '</tr>';
      tbody.innerHTML = '';
      (rows || []).forEach((row) => {
        const tr = document.createElement('tr');
        tr.innerHTML = columns.map((c) => {
          const v = row[c];
          const txt = (v == null) ? '' : String(v);
          const cls = (c.indexOf('uuid') >= 0 || c.indexOf('path') >= 0) ? ' class="mono"' : '';
          return '<td' + cls + '>' + txt + '</td>';
        }).join('');
        tbody.appendChild(tr);
      });
      if (!tbody.children.length) {
        tbody.innerHTML = '<tr><td colspan="' + Math.max(1, columns.length) + '">No rows for selected filters.</td></tr>';
      }
    }

    async function runReportQuery() {
      try {
        await loadReportOptions();
        const from = q('#report-date-from').value || '';
        const to = q('#report-date-to').value || '';
        const status = q('#report-status').value || 'all';
        const customerID = q('#report-customer-id').value || '';
        const limit = Math.max(1, Math.min(1000, Number(q('#report-limit').value || 100)));
        const offset = Math.max(0, Number(q('#report-offset').value || 0));
        const columns = selectedReportColumns();
        reportSelectedColumns = columns;

        const res = await fetch('/api/v1/reports/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            date_from: from,
            date_to: to,
            status: status,
            customer_id: customerID,
            limit: limit,
            offset: offset,
            columns: columns,
          }),
        });
        if (!res.ok) {
          const body = await res.json().catch(() => ({}));
          throw new Error(body.error || ('HTTP ' + res.status));
        }
        const payload = await res.json();
        reportRows = payload?.data || [];
        const activeColumns = payload?.meta?.columns || columns;
        renderReportResults(activeColumns, reportRows);
        text('report-summary', 'Rows: ' + (payload?.meta?.count || 0) + ' / Total: ' + (payload?.meta?.total || 0) + ' | Range: ' + (payload?.meta?.date_from || '-') + ' -> ' + (payload?.meta?.date_to || '-'));
        q('#report-debug-json').textContent = JSON.stringify({
          meta: payload?.meta || {},
          preview: reportRows.slice(0, 5),
        }, null, 2);
      } catch (err) {
        q('#report-results-body').innerHTML = '<tr><td>Failed: ' + err.message + '</td></tr>';
        q('#report-debug-json').textContent = 'Failed: ' + err.message;
      }
    }

    function exportReportCSV() {
      const columns = selectedReportColumns();
      if (!columns.length || !reportRows.length) return;
      const rows = reportRows.map((r) => columns.map((c) => r[c]));
      downloadCSV('transfer-report.csv', columns, rows);
    }

    async function saveReportTemplate() {
      try {
        const name = (q('#report-template-name').value || '').trim();
        if (!name) {
          q('#report-debug-json').textContent = 'Template name is required.';
          return;
        }
        const payload = {
          name: name,
          description: (q('#report-template-description').value || '').trim(),
          scope: 'transfer',
          config: {
            date_from: q('#report-date-from').value || '',
            date_to: q('#report-date-to').value || '',
            status: q('#report-status').value || 'all',
            customer_id: q('#report-customer-id').value || '',
            limit: Math.max(1, Math.min(1000, Number(q('#report-limit').value || 100))),
            offset: Math.max(0, Number(q('#report-offset').value || 0)),
            columns: selectedReportColumns(),
          },
        };
        const res = await fetch('/api/v1/reports/templates', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        const body = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(body.error || ('HTTP ' + res.status));
        q('#report-debug-json').textContent = JSON.stringify(body, null, 2);
        await loadReportTemplates();
        flashReportTemplate('[SAVE]', 'Template saved', true);
      } catch (err) {
        q('#report-debug-json').textContent = 'Failed: ' + err.message;
        flashReportTemplate('[ERR]', 'Save failed', false);
      }
    }

    async function loadSelectedTemplate() {
      try {
        const id = Number(q('#report-template-select').value || 0);
        if (!id) return;
        const res = await getJSON('/api/v1/reports/templates/' + encodeURIComponent(String(id)));
        const tpl = res?.data || {};
        const cfg = JSON.parse(tpl.config_json || '{}');
        q('#report-template-name').value = tpl.name || '';
        q('#report-template-description').value = tpl.description || '';
        q('#report-date-from').value = cfg.date_from || '';
        q('#report-date-to').value = cfg.date_to || '';
        q('#report-status').value = cfg.status || 'all';
        q('#report-customer-id').value = cfg.customer_id || '';
        q('#report-limit').value = cfg.limit || 100;
        q('#report-offset').value = cfg.offset || 0;
        reportSelectedColumns = Array.isArray(cfg.columns) ? cfg.columns : [];
        renderReportColumns(reportColumns);
        await runReportQuery();
        flashReportTemplate('[LOAD]', 'Template loaded', true);
      } catch (err) {
        q('#report-debug-json').textContent = 'Failed: ' + err.message;
        flashReportTemplate('[ERR]', 'Load failed', false);
      }
    }

    async function deleteSelectedTemplate() {
      try {
        const id = Number(q('#report-template-select').value || 0);
        if (!id) return;
        const res = await fetch('/api/v1/reports/templates/' + encodeURIComponent(String(id)), { method: 'DELETE' });
        const body = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(body.error || ('HTTP ' + res.status));
        q('#report-debug-json').textContent = JSON.stringify(body, null, 2);
        await loadReportTemplates();
        flashReportTemplate('[DEL]', 'Template deleted', true);
      } catch (err) {
        q('#report-debug-json').textContent = 'Failed: ' + err.message;
        flashReportTemplate('[ERR]', 'Delete failed', false);
      }
    }

    function resetAIPDetails() {
      aipCurrentStats = null;
      text('aip-d-uuid', '-');
      text('aip-d-sips', '-');
      text('aip-d-files', '-');
      text('aip-d-file-uuids', '-');
      text('aip-d-size', '-');
      text('aip-d-avg-size', '-');
      text('aip-d-created-range', '-');
      text('aip-d-indexed-range', '-');
      text('aip-d-event-range', '-');
      text('aip-d-with-norm', '-');
      text('aip-d-without-norm', '-');
      text('aip-d-norm-total', '-');
      text('aip-d-norm-avg', '-');
      text('aip-d-status-mix', '-');
      text('aip-d-origin-mix', '-');
      text('aip-d-accession-mix', '-');
      text('aip-d-ispartof-mix', '-');
      text('aip-d-am-version-mix', '-');
      text('aip-d-premis-type-mix', '-');
      text('aip-d-premis-outcome-mix', '-');
      text('aip-d-premis-tool-mix', '-');
      html('aip-d-unknown-formats', '-');
      html('aip-d-missing-identifiers', '-');
      html('aip-d-missing-created', '-');
      html('aip-d-ext-mismatch', '-');
      html('aip-d-dup-filenames', '-');
      html('aip-d-unique-formats', '-');
      html('aip-d-format-diversity', '-');
      html('aip-d-index-lag', '-');
      q('#aip-ext-body').innerHTML = '<tr><td colspan="2">Select an AIP to load extension/format stats.</td></tr>';
      q('#aip-pronom-body').innerHTML = '<tr><td colspan="2">Select an AIP to load format registry stats.</td></tr>';
      q('#aip-format-version-body').innerHTML = '<tr><td colspan="4">Select an AIP to load format versions.</td></tr>';
      q('#aip-largest-body').innerHTML = '<tr><td colspan="6">Select an AIP to load largest files.</td></tr>';
      q('#aip-ss-body').innerHTML = '<tr><td colspan="9">Select an AIP to load Storage Service package details.</td></tr>';
      q('#aip-largest-status-filter').innerHTML = '<option value="">All</option>';
      q('#aip-largest-ext-filter').value = '';
      q('#aip-largest-sort').value = 'size_desc';
    }

    function keyCountsToPretty(items, total, limit) {
      const rows = (items || []).slice(0, limit || 6);
      if (!rows.length) return '-';
      const denom = Math.max(1, Number(total || 0));
      return rows.map((it) => {
        const c = Number(it.count || 0);
        const p = ((c / denom) * 100).toFixed(1);
        return (it.key || '-') + ' (' + c + ', ' + p + '%)';
      }).join('<br>');
    }

    function totalFromKeyCounts(items) {
      return (items || []).reduce((acc, it) => acc + Number(it.count || 0), 0);
    }

    async function loadRiskThresholds() {
      try {
        const res = await getJSON('/api/v1/settings/risk-thresholds');
        const d = res?.data || {};
        riskThresholds = { ...riskThresholds, ...d };
      } catch (err) {
        console.error(err);
      }
    }

    function csvEscape(v) {
      const s = String(v ?? '');
      if (s.includes('"') || s.includes(',') || s.includes('\n')) return '"' + s.replaceAll('"', '""') + '"';
      return s;
    }

    function downloadCSV(filename, headers, rows) {
      const lines = [];
      lines.push(headers.map(csvEscape).join(','));
      rows.forEach((r) => lines.push(r.map(csvEscape).join(',')));
      const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(a.href);
    }

    function renderLargestFiles() {
      const lb = q('#aip-largest-body');
      lb.innerHTML = '';
      const d = aipCurrentStats || {};
      const all = Array.isArray(d.largest_files) ? [...d.largest_files] : [];
      const statusFilter = (q('#aip-largest-status-filter').value || '').trim().toLowerCase();
      const extFilter = (q('#aip-largest-ext-filter').value || '').trim().toLowerCase();
      const sortBy = q('#aip-largest-sort').value || 'size_desc';

      let rows = all.filter((it) => {
        const status = String(it.status || '').toLowerCase();
        const ext = String(it.extension || '').toLowerCase();
        if (statusFilter && status !== statusFilter) return false;
        if (extFilter && !ext.includes(extFilter)) return false;
        return true;
      });

      rows.sort((a, b) => {
        if (sortBy === 'size_asc') return Number(a.bytes || 0) - Number(b.bytes || 0);
        if (sortBy === 'path_asc') return String(a.file_path || '').localeCompare(String(b.file_path || ''));
        if (sortBy === 'path_desc') return String(b.file_path || '').localeCompare(String(a.file_path || ''));
        return Number(b.bytes || 0) - Number(a.bytes || 0);
      });

      rows.forEach((it) => {
        const tr = document.createElement('tr');
        tr.innerHTML =
          '<td title="' + (it.file_path || '-') + '">' + (it.file_path || '-') + '</td>' +
          '<td>' + ((it.bytes || 0) > 0 ? ((it.bytes / 1024 / 1024).toFixed(2) + ' MB') : '-') + '</td>' +
          '<td>' + ((it.format_registry_key || it.format_name) ? ((it.format_registry_key || '-') + ' / ' + (it.format_name || '-')) : '-') + '</td>' +
          '<td>' + (it.status || '-') + '</td>' +
          '<td>' + (it.created_by_app_date || '-') + '</td>' +
          '<td>' + (it.indexed_at || '-') + '</td>';
        lb.appendChild(tr);
      });
      if (!lb.children.length) lb.innerHTML = '<tr><td colspan="6">No rows matching filters.</td></tr>';
    }

    function bindLargestStatusFilter(stats) {
      const sel = q('#aip-largest-status-filter');
      const statuses = Array.from(new Set((stats.largest_files || []).map((it) => String(it.status || '').trim()).filter(Boolean))).sort();
      sel.innerHTML = '<option value="">All</option>';
      statuses.forEach((s) => {
        const opt = document.createElement('option');
        opt.value = s;
        opt.textContent = s;
        sel.appendChild(opt);
      });
    }

    async function loadAIPStorageService(aipUUID) {
      const body = q('#aip-ss-body');
      body.innerHTML = '<tr><td colspan="9">Loading Storage Service package details...</td></tr>';
      try {
        const res = await getJSON('/api/v1/aips/' + encodeURIComponent(aipUUID) + '/storage-service');
        const packages = res?.data?.packages || [];
        body.innerHTML = '';
        (packages || []).forEach((p) => {
          const tr = document.createElement('tr');
          tr.innerHTML =
            '<td class="mono">' + (p.uuid || '-') + '</td>' +
            '<td>' + (p.package_type || '-') + '</td>' +
            '<td>' + (p.status || '-') + '</td>' +
            '<td>' + ((p.size_bytes || 0) > 0 ? ((p.size_bytes / 1024 / 1024).toFixed(1) + ' MB') : '-') + '</td>' +
            '<td>' + (p.stored_date ? String(p.stored_date).replace('T', ' ').replace('Z', '') : '-') + '</td>' +
            '<td>' + (p.current_location || '-') + '</td>' +
            '<td title="' + (p.current_path || '-') + '">' + (p.current_path || '-') + '</td>' +
            '<td>' + (p.pipeline_name || p.pipeline_uuid || '-') + '</td>' +
            '<td>' + (p.files_count || 0) + '/' + (p.files_stored_count || 0) + '</td>';
          body.appendChild(tr);
        });
        if (!body.children.length) {
          body.innerHTML = '<tr><td colspan="9">No Storage Service package found for this AIP UUID.</td></tr>';
        }
      } catch (err) {
        console.error(err);
        body.innerHTML = '<tr><td colspan="9">Failed to load Storage Service data.</td></tr>';
      }
    }

    async function loadAIPStats(aipUUID) {
      resetAIPDetails();
      text('aip-d-uuid', aipUUID);
      try {
        const res = await getJSON('/api/v1/aips/' + encodeURIComponent(aipUUID) + '/stats');
        const d = res?.data || {};
        aipCurrentStats = d;
        text('aip-d-uuid', d.aip_uuid || aipUUID);
        text('aip-d-sips', (d.sip_names || []).join(', ') || '-');
        text('aip-d-files', String(d.files_total || 0));
        text('aip-d-file-uuids', String(d.unique_file_uuids || 0));
        text('aip-d-size', (d.total_bytes || 0) > 0 ? ((d.total_bytes / 1024 / 1024).toFixed(1) + ' MB') : '-');
        text('aip-d-avg-size', (d.average_bytes || 0) > 0 ? ((d.average_bytes / 1024 / 1024).toFixed(2) + ' MB') : '-');
        text('aip-d-created-range', (d.min_created_by_app_date || '-') + ' -> ' + (d.max_created_by_app_date || '-'));
        text('aip-d-indexed-range', (d.min_indexed_at || '-') + ' -> ' + (d.max_indexed_at || '-'));
        text('aip-d-event-range', (d.first_event_date || '-') + ' -> ' + (d.last_event_date || '-'));
        text('aip-d-with-norm', String(d.originals_with_normalized || 0));
        text('aip-d-without-norm', String(d.originals_without_normalized || 0));
        text('aip-d-norm-total', String(d.normalized_refs_total || 0));
        text('aip-d-norm-avg', String(d.normalized_by_original_avg || 0));
        const totalFiles = Number(d.files_total || 0);
        html('aip-d-status-mix', keyCountsToPretty(d.status_counts, totalFiles, 8));
        html('aip-d-origin-mix', keyCountsToPretty(d.origin_counts, totalFiles, 6));
        html('aip-d-accession-mix', keyCountsToPretty(d.accession_id_counts, totalFiles, 6));
        html('aip-d-ispartof-mix', keyCountsToPretty(d.is_part_of_counts, totalFiles, 6));
        html('aip-d-am-version-mix', keyCountsToPretty(d.archivematica_version_counts, totalFiles, 6));
        html('aip-d-premis-type-mix', keyCountsToPretty(d.premis_event_type_counts, totalFromKeyCounts(d.premis_event_type_counts), 8));
        html('aip-d-premis-outcome-mix', keyCountsToPretty(d.premis_event_outcome_counts, totalFromKeyCounts(d.premis_event_outcome_counts), 8));
        html('aip-d-premis-tool-mix', keyCountsToPretty(d.premis_tool_counts, totalFromKeyCounts(d.premis_tool_counts), 8));
        const unknownFormats = Number(d.unknown_formats || 0);
        const missingIDs = Number(d.missing_identifiers || 0);
        const missingCreated = Number(d.missing_created_by_app_date || 0);
        const mismatches = Number(d.extension_format_mismatch || 0);
        const dupGroups = Number(d.duplicate_filename_groups || 0);
        const dupFiles = Number(d.duplicate_filename_candidates || 0);
        const uniqueFormats = Number(d.unique_format_signatures || 0);
        const diversityRatio = Number(d.format_diversity_ratio || 0);

        const unknownRisk = isRiskByRate(unknownFormats, totalFiles, riskThresholds.unknown_hot_rate, riskThresholds.unknown_hot_abs);
        const missingIDsRisk = isRiskByRate(missingIDs, totalFiles, riskThresholds.missing_ids_hot_rate, riskThresholds.missing_ids_hot_abs);
        const missingCreatedRisk = isRiskByRate(missingCreated, totalFiles, riskThresholds.missing_created_hot_rate, riskThresholds.missing_created_hot_abs);
        const mismatchRisk = isRiskByRate(mismatches, totalFiles, riskThresholds.ext_mismatch_hot_rate, riskThresholds.ext_mismatch_hot_abs);
        const dupRisk = isRiskByRate(dupFiles, totalFiles, riskThresholds.dup_files_hot_rate, riskThresholds.dup_files_hot_abs) || dupGroups >= Number(riskThresholds.dup_groups_hot_abs || 0);
        const uniqueFormatsOK = totalFiles <= Number(riskThresholds.tiny_files_max || 20) ? (uniqueFormats >= Number(riskThresholds.min_unique_formats_for_tiny || 1)) : (uniqueFormats >= Number(riskThresholds.min_unique_formats || 2));
        const diversityOK = totalFiles <= Number(riskThresholds.tiny_files_max || 20) ? true : (diversityRatio >= Number(riskThresholds.min_diversity_ratio || 0.02));

        html('aip-d-unknown-formats', riskBadge(!unknownRisk, String(unknownFormats)));
        html('aip-d-missing-identifiers', riskBadge(!missingIDsRisk, String(missingIDs)));
        html('aip-d-missing-created', riskBadge(!missingCreatedRisk, String(missingCreated)));
        html('aip-d-ext-mismatch', riskBadge(!mismatchRisk, String(mismatches)));
        html('aip-d-dup-filenames', riskBadge(!dupRisk, String(dupGroups) + ' groups / ' + String(dupFiles) + ' files'));
        html('aip-d-unique-formats', riskBadge(uniqueFormatsOK, String(uniqueFormats)));
        html('aip-d-format-diversity', riskBadge(diversityOK, String(diversityRatio)));
        const lagP95 = Number(d.indexed_lag_p95_seconds || 0);
        html('aip-d-index-lag', riskBadge(lagP95 <= Number(riskThresholds.index_lag_p95_hot_sec || 1800), 'avg=' + fmtSec(d.indexed_lag_avg_seconds || 0) + ', p95=' + fmtSec(lagP95)));

        const eb = q('#aip-ext-body');
        eb.innerHTML = '';
        (d.extension_counts || []).forEach((it) => {
          const tr = document.createElement('tr');
          tr.innerHTML = '<td>' + (it.key || '-') + '</td><td>' + (it.count || 0) + '</td>';
          eb.appendChild(tr);
        });
        if (!eb.children.length) eb.innerHTML = '<tr><td colspan="2">No extension stats found.</td></tr>';

        const pb = q('#aip-pronom-body');
        pb.innerHTML = '';
        (d.format_registry_counts || []).forEach((it) => {
          const tr = document.createElement('tr');
          tr.innerHTML = '<td>' + (it.key || '-') + '</td><td>' + (it.count || 0) + '</td>';
          pb.appendChild(tr);
        });
        if (!pb.children.length) pb.innerHTML = '<tr><td colspan="2">No format registry stats found.</td></tr>';

        const fvb = q('#aip-format-version-body');
        fvb.innerHTML = '';
        (d.format_version_counts || []).forEach((it) => {
          const tr = document.createElement('tr');
          tr.innerHTML =
            '<td>' + (it.format_registry_key || '-') + '</td>' +
            '<td>' + (it.format_name || '-') + '</td>' +
            '<td>' + (it.format_version || '-') + '</td>' +
            '<td>' + (it.count || 0) + '</td>';
          fvb.appendChild(tr);
        });
        if (!fvb.children.length) fvb.innerHTML = '<tr><td colspan="4">No format version stats found.</td></tr>';

        bindLargestStatusFilter(d);
        renderLargestFiles();
        loadAIPStorageService(d.aip_uuid || aipUUID);
      } catch (err) {
        console.error(err);
      }
    }

    async function loadAIPList(reset) {
      try {
        const body = q('#aip-list-body');
        if (reset) {
          aipCursor = '';
          body.innerHTML = '';
        }
        let url = '/api/v1/aips?limit=60';
        if (aipCursor) {
          url += '&cursor=' + encodeURIComponent(aipCursor);
        }
        const res = await getJSON(url);
        const items = res?.data || [];
        aipCursor = res?.meta?.next_cursor || '';

        items.forEach((item) => {
          const tr = document.createElement('tr');
          tr.className = 'row-click';
          tr.innerHTML = '<td class="mono">' + (item.aip_uuid || '-') + '</td><td>' + (item.sip_name || '-') + '</td>';
          tr.onclick = () => loadAIPStats(item.aip_uuid);
          body.appendChild(tr);
        });
        if (!body.children.length) body.innerHTML = '<tr><td colspan="2">No AIPs found in Elasticsearch.</td></tr>';
        q('#aip-load-more').disabled = !aipCursor;
      } catch (err) {
        console.error(err);
      }
    }

	    function resetTransferSummary() {
      text('ds-uuid', '-');
      text('ds-name', '-');
      text('ds-status', '-');
      text('ds-duration', '-');
      text('ds-files', '-');
      text('ds-failed-jobs', '-');
      text('ds-tasks', '-');
      text('ds-size', '-');
      text('ds-timeline', '-');
      text('ds-errors', '-');
      text('ds-bottleneck', '-');
      text('ds-ss-packages', '-');
      text('ds-es', '-');
      const ssb = q('#sspkg-body');
      ssb.innerHTML = '<tr><td colspan="7">Click a completed transfer to load Storage Service package details.</td></tr>';
	      const pb = q('#perf-body');
	      pb.innerHTML = '<tr><td colspan="7">Click a completed transfer to load microservice performance.</td></tr>';
	    }

	    function selectRow(tableBodySelector, rowEl) {
	      const body = q(tableBodySelector);
	      if (!body) return;
	      Array.from(body.querySelectorAll('tr.row-selected')).forEach((r) => r.classList.remove('row-selected'));
	      if (rowEl) rowEl.classList.add('row-selected');
	    }

    function renderStorageServicePackages(packages) {
      const ssb = q('#sspkg-body');
      ssb.innerHTML = '';
      (packages || []).forEach((p) => {
        const tr = document.createElement('tr');
        tr.innerHTML =
          '<td class="mono">' + (p.uuid || '-') + '</td>' +
          '<td>' + (p.package_type || '-') + '</td>' +
          '<td>' + (p.status || '-') + '</td>' +
          '<td>' + ((p.size_bytes || 0) > 0 ? ((p.size_bytes / 1024 / 1024).toFixed(1) + ' MB') : '-') + '</td>' +
          '<td>' + (p.current_location || '-') + '</td>' +
          '<td title="' + (p.current_path || '-') + '">' + (p.current_path || '-') + '</td>' +
          '<td>' + (p.files_count || 0) + '</td>';
        ssb.appendChild(tr);
      });
      if (!ssb.children.length) ssb.innerHTML = '<tr><td colspan="7">No Storage Service packages found for this transfer/SIP UUID.</td></tr>';
    }

	    function renderPerformanceRows(microservices) {
	      const pb = q('#perf-body');
	      pb.innerHTML = '';
	      (microservices || []).forEach((m) => {
	        const ratio = Number(m.cpu_to_wall_ratio || 0);
	        const hint = String(m.bottleneck_hint || '-');
	        const failedTasks = Number(m.failed_tasks || 0);
	        const isFailureRow = failedTasks > 0;
	        const tr = document.createElement('tr');
	        if (isFailureRow) tr.className = 'row-failed';
	        tr.innerHTML =
	          '<td>' + (m.phase || '-') + '</td>' +
	          '<td title="' + (m.microservice_group || '-') + '">' + (m.microservice_group || '-') + '</td>' +
	          '<td>' + fmtSec(m.cpu_seconds || 0) + '</td>' +
	          '<td>' + fmtSec(m.duration_seconds || 0) + '</td>' +
	          '<td>' + (m.tasks || 0) + (failedTasks > 0 ? (' <span class="pill bad">failed:' + failedTasks + '</span>') : '') + '</td>' +
	          '<td>' + ratio.toFixed(2) + '</td>' +
	          '<td>' + (isFailureRow ? '<span class="pill bad">FAILED</span> ' : '') + hint + '</td>';
	        pb.appendChild(tr);
	      });
	      if (!pb.children.length) pb.innerHTML = '<tr><td colspan="7">No microservice performance data found.</td></tr>';
	    }

    function updateTransferSummary(details) {
      const summary = details?.data?.summary || {};
      const performance = details?.data?.performance || {};
      const perfDetails = performance.transfer_details || {};
      const perfSize = performance.transfer_size || {};
      const perfSummary = performance.summary || {};
      const ss = details?.data?.storage_service || {};
      const meta = details?.meta || {};

      text('ds-uuid', summary.transfer_uuid || meta.transfer_uuid || '-');
      text('ds-name', summary.name || '-');
      text('ds-status', summary.status || '-');
      text('ds-duration', fmtSec(summary.duration_seconds || 0));
      text('ds-files', String(summary.files_total || 0));
      text('ds-failed-jobs', String(summary.failed_jobs || 0));
      text('ds-tasks', String(perfDetails.tasks || 0));
      text('ds-size', (perfSize.total_mb || 0) + ' MB');
      text('ds-timeline', String(meta.timeline_count || 0));
      text('ds-errors', String(meta.error_count || 0));
      const longestName = perfSummary.longest_microservice_group || '-';
      const longestSec = perfSummary.longest_microservice_seconds || 0;
      text('ds-bottleneck', longestName + (longestSec > 0 ? (' (' + fmtSec(longestSec) + ')') : ''));
      text('ds-ss-packages', String(meta.ss_packages || 0));
      text('ds-es', meta.es_hits != null ? String(meta.es_hits) : (meta.es_error ? ('error: ' + meta.es_error) : '0'));
      renderStorageServicePackages(ss.packages || []);
      renderPerformanceRows(performance.microservices || []);
    }

    async function loadTransferDetails(transferUUID) {
	      const box = q('#details-json');
	      box.textContent = 'Loading ' + transferUUID + ' ...';
	      try {
	        const details = await getJSON('/api/v1/transfers/' + encodeURIComponent(transferUUID) + '/details?limit=100');
	        updateTransferSummary(details);
	        const summaryView = {
	          meta: details?.meta || {},
	          summary: details?.data?.summary || {},
	          performance_summary: details?.data?.performance?.summary || {},
	          transfer_details: details?.data?.performance?.transfer_details || {},
	          transfer_size: details?.data?.performance?.transfer_size || {},
	          storage_service: {
	            lookup_uuids: details?.data?.storage_service?.lookup_uuids || [],
	            packages_count: (details?.data?.storage_service?.packages || []).length,
	          },
	          elasticsearch: {
	            total_hits: details?.data?.elasticsearch?.total_hits || 0,
	            took_ms: details?.data?.elasticsearch?.took_ms || 0,
	          },
	          timeline_preview: (details?.data?.timeline || []).slice(0, 6),
	          errors_preview: (details?.data?.errors || []).slice(0, 8),
	        };
	        box.textContent = JSON.stringify(summaryView, null, 2);
	      } catch (err) {
	        resetTransferSummary();
	        box.textContent = 'Failed: ' + err.message;
	      }
	    }

	    async function loadFailedTransferDetails(transferUUID, rowContext) {
	      const box = q('#failed-details-json');
	      const selectedInfo = q('#failed-selected-info');
	      const taskBody = q('#failed-task-body');
	      const contextView = {
	        transfer_uuid: transferUUID,
	        failed_row_context: rowContext || {},
	      };
	      const group = (rowContext && rowContext.microservice_group) ? rowContext.microservice_group : '-';
	      selectedInfo.textContent = 'Selected: ' + transferUUID + ' | Microservice: ' + group;
	      taskBody.innerHTML = '<tr><td colspan="6">Loading failed task rows...</td></tr>';
	      box.textContent = JSON.stringify(contextView, null, 2) + '\n\nLoading full details...';
	      try {
	        const details = await getJSON('/api/v1/transfers/' + encodeURIComponent(transferUUID) + '/details?limit=250');
	        const failedRows = details?.data?.errors || [];
	        renderFailedTaskRows(failedRows);
	        const summaryView = {
	          failed_row_context: rowContext || {},
	          meta: details?.meta || {},
	          summary: details?.data?.summary || {},
	          performance_summary: details?.data?.performance?.summary || {},
	          transfer_details: details?.data?.performance?.transfer_details || {},
	          transfer_size: details?.data?.performance?.transfer_size || {},
	          timeline_preview: (details?.data?.timeline || []).slice(0, 6),
	          failed_tasks_total: failedRows.length,
	          errors_preview: failedRows.slice(0, 8),
	        };
	        box.textContent = JSON.stringify(summaryView, null, 2);
	      } catch (err) {
	        taskBody.innerHTML = '<tr><td colspan="6">Failed to load failed task rows.</td></tr>';
	        box.textContent = 'Failed: ' + err.message;
	      }
	    }

	    function renderFailedTaskRows(rows) {
	      const body = q('#failed-task-body');
	      body.innerHTML = '';
	      (rows || []).forEach((e) => {
	        const tr = document.createElement('tr');

	        const ended = document.createElement('td');
	        ended.textContent = e.ended_at ? String(e.ended_at).replace('T', ' ').replace('Z', '') : '-';

	        const micro = document.createElement('td');
	        micro.textContent = e.microservice_group || '-';

	        const job = document.createElement('td');
	        job.textContent = e.job_type || '-';

	        const file = document.createElement('td');
	        const fileParts = [];
	        if (e.file_path) fileParts.push(e.file_path);
	        if (e.file_uuid) fileParts.push('[' + e.file_uuid + ']');
	        file.textContent = fileParts.length ? fileParts.join(' ') : '-';
	        file.className = 'mono';

	        const exit = document.createElement('td');
	        exit.textContent = (e.exit_code != null) ? String(e.exit_code) : '-';

	        const out = document.createElement('td');
	        const stderr = String(e.stderr || e.error_text || '').trim();
	        const stdout = String(e.stdout || '').trim();
	        let txt = stderr;
	        if (!txt) txt = stdout;
	        if (!txt) txt = '(no stderr/stdout text)';
	        if (txt.length > 260) txt = txt.slice(0, 260) + '...';
	        out.textContent = txt;
	        out.title = [stderr, stdout, e.arguments || '', e.execution || ''].filter(Boolean).join('\n\n');

	        tr.appendChild(ended);
	        tr.appendChild(micro);
	        tr.appendChild(job);
	        tr.appendChild(file);
	        tr.appendChild(exit);
	        tr.appendChild(out);
	        body.appendChild(tr);
	      });
	      if (!body.children.length) body.innerHTML = '<tr><td colspan="6">No failed task rows found for this transfer.</td></tr>';
	    }

	    async function loadFailedTab() {
	      try {
	        text('failed-recent-title', 'Recent Failed Transfers (' + failureWindowLabel() + ')');
	        text('failed-signatures-title', 'Global Failure Signatures (' + failureWindowLabel() + ', all failed transfers)');
	        const [failed, signatures] = await Promise.all([
	          getJSON('/api/v1/transfers/failed?hours=' + encodeURIComponent(failureWindow) + '&limit=30'),
	          getJSON('/api/v1/troubleshooting/failure-signatures?hours=' + encodeURIComponent(failureWindow) + '&limit=30')
	        ]);

	        const fb = q('#failed-body');
	        fb.innerHTML = '';
		        (failed.data || []).forEach((t) => {
		          const status = String(t.status || '-');
		          const isRecovered = !!t.recoverable || status.indexOf('NON_BLOCKING') >= 0;
		          const isFailed = !isRecovered;
	          const tr = document.createElement('tr');
	          tr.className = 'row-click' + (isRecovered ? ' row-recovered' : (isFailed ? ' row-failed' : ''));
	          tr.dataset.transferUuid = t.transfer_uuid || '';
	          tr.dataset.microserviceGroup = t.microservice_group || '';
	          tr.dataset.errorText = t.error_text || '';
	          tr.dataset.status = status;
	          const c1 = document.createElement('td');
	          c1.className = 'mono';
	          c1.textContent = t.name || t.transfer_uuid || '-';
	          const c2 = document.createElement('td');
	          c2.innerHTML = '<span class="pill ' + (isRecovered ? 'warn' : 'bad') + '">' + status + '</span>';
	          const c3 = document.createElement('td');
	          c3.textContent = t.microservice_group || '-';
	          const c4 = document.createElement('td');
	          c4.textContent = t.failed_at ? String(t.failed_at).replace('T', ' ').replace('Z', '') : '-';
	          const c5 = document.createElement('td');
	          c5.textContent = fmtSec(t.duration_seconds || 0);
	          const c6 = document.createElement('td');
	          c6.textContent = String(t.files_total || 0);
	          const c7 = document.createElement('td');
	          c7.textContent = String(t.failed_jobs || 0);
	          const c8 = document.createElement('td');
	          const a = document.createElement('a');
	          a.href = '/api/v1/transfers/' + encodeURIComponent(t.transfer_uuid || '') + '/details?limit=60';
	          a.target = '_blank';
	          a.rel = 'noopener noreferrer';
	          a.textContent = 'open';
	          c8.appendChild(a);
	          tr.appendChild(c1);
	          tr.appendChild(c2);
	          tr.appendChild(c3);
	          tr.appendChild(c4);
	          tr.appendChild(c5);
	          tr.appendChild(c6);
	          tr.appendChild(c7);
	          tr.appendChild(c8);
	          fb.appendChild(tr);
	        });
	        if (!fb.children.length) fb.innerHTML = '<tr><td colspan="8">No failed transfers in the selected window.</td></tr>';

	        const sb = q('#failed-signature-body');
	        sb.innerHTML = '';
	        (signatures.data || []).forEach((s) => {
	          const tr = document.createElement('tr');
	          const c1 = document.createElement('td');
	          c1.textContent = s.microservice_group || '-';
	          const c2 = document.createElement('td');
	          c2.title = s.signature || '-';
	          c2.textContent = s.signature || '-';
	          const c3 = document.createElement('td');
	          c3.textContent = String(s.failures || 0);
	          const c4 = document.createElement('td');
	          c4.textContent = String(s.distinct_transfers || 0);
	          const c5 = document.createElement('td');
	          c5.textContent = s.last_seen_at ? String(s.last_seen_at).replace('T', ' ').replace('Z', '') : '-';
	          tr.appendChild(c1);
	          tr.appendChild(c2);
	          tr.appendChild(c3);
	          tr.appendChild(c4);
	          tr.appendChild(c5);
	          sb.appendChild(tr);
	        });
        if (!sb.children.length) sb.innerHTML = '<tr><td colspan="5">No grouped signatures in the selected window.</td></tr>';
      } catch (err) {
        console.error(err);
      }
    }

    async function loadServicesStatus() {
      try {
        const [res, appMetricsRes] = await Promise.all([
          getJSON('/api/v1/status/services'),
          getJSON('/api/v1/metrics/app')
        ]);
        const services = res?.services || {};
        const appMetrics = appMetricsRes?.data || {};
        const mysql = services.mysql || {};
        const ssdb = services.storage_service_db || {};
        const es = services.elasticsearch || {};
        const prom = services.prometheus || {};

        text('svc-mysql', mysql.ok ? 'UP' : 'DOWN');
        text('svc-es', es.ok ? 'UP' : 'DOWN');
        text('svc-prom-up', (prom.targets_up || 0) + '/' + (prom.targets_total || 0));
        text('svc-updated', res?.generated_at ? res.generated_at.replace('T', ' ').replace('Z', '') : '-');

        const coreBody = q('#services-core-body');
        coreBody.innerHTML = '';

        const mysqlStats = mysql.stats || {};
        const mysqlRow = document.createElement('tr');
        mysqlRow.innerHTML =
          '<td>MySQL</td>' +
          '<td>' + statusPill(!!mysql.ok) + '</td>' +
          '<td>' + (mysqlStats.ping_ms != null ? mysqlStats.ping_ms + 'ms' : '-') + '</td>' +
          '<td>' + fmtDuration(mysqlStats.uptime_seconds || 0) + '</td>' +
          '<td>running=' + (mysqlStats.transfers_running || 0) + ', completed=' + (mysqlStats.transfers_completed || 0) + ', failed24h=' + (mysqlStats.jobs_failed_24h || 0) + '</td>';
        coreBody.appendChild(mysqlRow);

        const esStats = es.stats || {};
        const esRow = document.createElement('tr');
        esRow.innerHTML =
          '<td>Elasticsearch</td>' +
          '<td>' + statusPill(!!es.ok) + '</td>' +
          '<td>' + (esStats.ping_ms != null ? esStats.ping_ms + 'ms' : '-') + '</td>' +
          '<td>' + fmtDuration(esStats.node_uptime_seconds || 0) + '</td>' +
          '<td>cluster=' + (esStats.cluster_status || '-') + ', nodes=' + (esStats.node_count || 0) + ', shards=' + (esStats.active_shards || 0) + '/' + (esStats.unassigned_shards || 0) + '</td>';
        coreBody.appendChild(esRow);

        const ssStats = ssdb.stats || {};
        const ssRow = document.createElement('tr');
        ssRow.innerHTML =
          '<td>Storage Service DB</td>' +
          '<td>' + statusPill(!!ssdb.ok) + '</td>' +
          '<td>' + (ssStats.ping_ms != null ? ssStats.ping_ms + 'ms' : '-') + '</td>' +
          '<td>' + fmtDuration(ssStats.uptime_seconds || 0) + '</td>' +
          '<td>packages=' + (ssStats.packages_total || 0) + ', locations=' + (ssStats.locations || 0) + ', files=' + (ssStats.files_total || 0) + '</td>';
        coreBody.appendChild(ssRow);

        const promBody = q('#services-prom-body');
        promBody.innerHTML = '';
        (prom.targets || []).forEach((target) => {
          const tr = document.createElement('tr');
          const cpuPct = Number(target.cpu_percent || 0);
          const memMB = Number(target.memory_mb || 0);
          const cpuClass = classifyMetric(cpuPct, 60, 80);
          const memClass = classifyMetric(memMB, 512, 1024);
          const gorClass = classifyMetric(Number(target.goroutines || 0), 300, 800);
          tr.innerHTML =
            '<td class="mono">' + (target.target || '-') + '</td>' +
            '<td>' + statusPill(!!target.ok) + '</td>' +
            '<td>' + (target.ping_ms != null ? target.ping_ms + 'ms' : '-') + '</td>' +
            '<td>' + fmtDuration(target.uptime_seconds || 0) + '</td>' +
            '<td class="' + cpuClass + '">' + (cpuPct > 0 ? cpuPct.toFixed(1) + '%' : '-') + '</td>' +
            '<td class="' + memClass + '">' + (memMB > 0 ? memMB.toFixed(1) + ' MB' : '-') + '</td>' +
            '<td class="' + gorClass + '">' + (target.goroutines || 0) + '</td>' +
            '<td>' + (target.sample_count || 0) + '</td>';
          promBody.appendChild(tr);
        });
        if (!promBody.children.length) promBody.innerHTML = '<tr><td colspan="8">No Prometheus targets configured.</td></tr>';

        const appHTTPBody = q('#services-app-http-body');
        appHTTPBody.innerHTML = '';
        (appMetrics.top_http_slowest_avg_ms || []).forEach((row) => {
          const tr = document.createElement('tr');
          tr.innerHTML =
            '<td>' + (row.method || '-') + '</td>' +
            '<td class="mono">' + (row.path || '-') + '</td>' +
            '<td>' + (row.status || '-') + '</td>' +
            '<td>' + (row.count || 0) + '</td>' +
            '<td>' + Number(row.avg_ms || 0).toFixed(2) + '</td>';
          appHTTPBody.appendChild(tr);
        });
        if (!appHTTPBody.children.length) appHTTPBody.innerHTML = '<tr><td colspan="5">No app HTTP metrics yet.</td></tr>';

        const appDBBody = q('#services-app-db-body');
        appDBBody.innerHTML = '';
        (appMetrics.top_db_slowest_avg_ms || []).forEach((row) => {
          const tr = document.createElement('tr');
          tr.innerHTML =
            '<td>' + (row.connector || '-') + '</td>' +
            '<td class="mono">' + (row.operation || '-') + '</td>' +
            '<td>' + (row.count || 0) + '</td>' +
            '<td>' + (row.errors || 0) + '</td>' +
            '<td>' + Number(row.avg_ms || 0).toFixed(2) + '</td>';
          appDBBody.appendChild(tr);
        });
        if (!appDBBody.children.length) appDBBody.innerHTML = '<tr><td colspan="5">No app DB metrics yet.</td></tr>';
        const errors = appMetrics.errors || {};
        text('services-app-errors', 'Errors: db=' + (errors.db_query_total || 0) + ', external=' + (errors.external_probe_total || 0));
      } catch (err) {
        console.error(err);
      }
    }

    function drawSeries(canvas, seriesA, seriesB, colorA, colorB) {
      const c = canvas.getContext('2d');
      const w = canvas.width, h = canvas.height;
      c.clearRect(0, 0, w, h);
      c.fillStyle = '#fff';
      c.fillRect(0, 0, w, h);

      const pad = 24;
      const all = [...seriesA, ...seriesB].map(x => x.y).filter(Number.isFinite);
      const max = Math.max(1, ...all);

      c.strokeStyle = '#eee';
      for (let i = 0; i < 4; i++) {
        const y = pad + ((h - pad * 2) * i / 3);
        c.beginPath();
        c.moveTo(pad, y);
        c.lineTo(w - pad, y);
        c.stroke();
      }

      function draw(series, color) {
        if (!series.length) return;
        c.strokeStyle = color;
        c.lineWidth = 2;
        c.beginPath();
        series.forEach((p, i) => {
          const x = pad + ((w - pad * 2) * (series.length === 1 ? 0 : i / (series.length - 1)));
          const y = h - pad - ((h - pad * 2) * (p.y / max));
          if (i === 0) c.moveTo(x, y); else c.lineTo(x, y);
        });
        c.stroke();
      }

      draw(seriesA, colorA);
      draw(seriesB, colorB);
    }

    async function load() {
      try {
        const [running, runningSIPs, stalled, failureCounts, hotspotsTransfer, hotspotsSIP, durations, promLive, promChart, completed] = await Promise.all([
          getJSON('/api/v1/transfers/running?limit=12'),
          getJSON('/api/v1/sips/running?limit=12'),
          getJSON('/api/v1/troubleshooting/stalled?limit=12'),
          getJSON('/api/v1/troubleshooting/failure-counts?hours=' + encodeURIComponent(failureWindow)),
          getJSON('/api/v1/troubleshooting/hotspots?unit=transfer&hours=' + encodeURIComponent(failureWindow) + '&limit=10'),
          getJSON('/api/v1/troubleshooting/hotspots?unit=sip&hours=' + encodeURIComponent(failureWindow) + '&limit=10'),
          getJSON('/api/v1/charts/transfer-durations?customer_id=all&month=' + new Date().toISOString().slice(0, 7)),
          getJSON('/api/v1/metrics/prometheus/live?match=mcp'),
          getJSON('/api/v1/charts/prometheus?target=' + encodeURIComponent('http://127.0.0.1:62992/metrics') + '&metric=mcpclient_job_total&minutes=120'),
          getJSON('/api/v1/transfers/completed?limit=20')
        ]);

        const runningTransfers = Number(running.meta?.count ?? 0);
        const runningSips = Number(runningSIPs.meta?.count ?? 0);
        const stalledTransfers = Number(stalled.meta?.count ?? 0);
        const stalledSips = (runningSIPs.data || []).filter((s) => !!s.stuck).length;
        const transferFailuresWindow = Number(failureCounts.data?.transfer?.failed_units ?? 0);
        const sipFailuresWindow = Number(failureCounts.data?.sip?.failed_units ?? 0);
        const failuresWindow = transferFailuresWindow + sipFailuresWindow;
        text('workflow-failure-col', 'Failures (' + failureWindowLabel() + ')');
        const summaryBody = q('#workflow-summary-body');
        summaryBody.innerHTML = '';
        const rows = [
          { label: 'TRANSFER', running: runningTransfers, stalled: stalledTransfers, failures: transferFailuresWindow },
          { label: 'SIP', running: runningSips, stalled: stalledSips, failures: sipFailuresWindow },
          { label: 'TOTAL', running: runningTransfers + runningSips, stalled: stalledTransfers + stalledSips, failures: failuresWindow },
        ];
        rows.forEach((r) => {
          const tr = document.createElement('tr');
          tr.innerHTML =
            '<td><span class="pill ' + (r.label === 'TOTAL' ? 'warn' : 'ok') + '">' + r.label + '</span></td>' +
            '<td>' + r.running + '</td>' +
            '<td>' + r.stalled + '</td>' +
            '<td>' + r.failures + '</td>';
          summaryBody.appendChild(tr);
        });

        const rb = q('#running-body');
        rb.innerHTML = '';
        (running.data || []).forEach(r => {
          const status = String(r.status || '');
          const statusClass = status === 'WAITING' ? 'info' : (r.stuck ? 'bad' : 'ok');
          const tr = document.createElement('tr');
          tr.innerHTML = '<td><span class="pill ok">TRANSFER</span></td>' +
            '<td class="mono">' + (r.name || r.transfer_uuid) + '</td>' +
            '<td>' + (r.stage || '-') + '</td>' +
            '<td><span class="pill ' + statusClass + '">' + status + '</span></td>' +
            '<td>' + fmtSec(r.elapsed_seconds || 0) + '</td>';
          rb.appendChild(tr);
        });
        (runningSIPs.data || []).forEach(s => {
          const status = String(s.status || '');
          const statusClass = status === 'WAITING' ? 'info' : (s.stuck ? 'bad' : 'ok');
          const tr = document.createElement('tr');
          tr.innerHTML = '<td><span class="pill warn">SIP</span></td>' +
            '<td class="mono">' + (s.sip_uuid || '-') + '</td>' +
            '<td>' + (s.stage || '-') + '</td>' +
            '<td><span class="pill ' + statusClass + '">' + status + '</span></td>' +
            '<td>' + fmtSec(s.elapsed_seconds || 0) + '</td>';
          rb.appendChild(tr);
        });
        if (!rb.children.length) rb.innerHTML = '<tr><td colspan="5">No running transfers or SIPs.</td></tr>';

        const hb = q('#hotspot-body');
        hb.innerHTML = '';
        (hotspotsTransfer.data || []).forEach(h => {
          const tr = document.createElement('tr');
          tr.innerHTML = '<td>' + h.microservice_group + '</td><td>' + h.failures + '</td><td>' + h.distinct_transfers + '</td>';
          hb.appendChild(tr);
        });
        if (!hb.children.length) hb.innerHTML = '<tr><td colspan="3">No recent failures.</td></tr>';

        const durA = (durations.data || []).map(d => ({ x: d.date, y: d.avg_seconds || 0 }));
        const durB = (durations.data || []).map(d => ({ x: d.date, y: d.p95_seconds || 0 }));
        drawSeries(q('#duration-chart'), durA, durB, '#0e5d8f', '#cb4b16');

        const p = (promChart.data || []).map(d => ({ x: d.timestamp, y: d.value || 0 }));
        drawSeries(q('#prom-chart'), p, [], '#0971b2', '#000');

		        const cb = q('#completed-body');
		        cb.innerHTML = '';
		        (completed.data || []).forEach(t => {
		          const statusText = String(t.status || '');
		          const isRecovered = !!t.recoverable || statusText.indexOf('NON_BLOCKING') >= 0;
		          const isFailed = !isRecovered && (!!t.failure_evidence || t.status_code === 4 || statusText.indexOf('FAILED') >= 0);
		          const tr = document.createElement('tr');
		          tr.className = 'row-click' + (isRecovered ? ' row-recovered' : (isFailed ? ' row-failed' : ''));
		          tr.innerHTML =
		            '<td class="mono">' + (t.name || t.transfer_uuid) + '</td>' +
		            '<td><span class="pill ' + (isRecovered ? 'warn' : (isFailed ? 'bad' : 'ok')) + '">' + (t.status || '-') + '</span></td>' +
		            '<td>' + (t.completed_at ? t.completed_at.replace('T', ' ').replace('Z', '') : '-') + '</td>' +
		            '<td>' + fmtSec(t.duration_seconds || 0) + '</td>' +
		            '<td>' + (t.files_total || 0) + '</td>';
	          tr.onclick = () => {
	            selectRow('#completed-body', tr);
	            loadTransferDetails(t.transfer_uuid);
	          };
	          cb.appendChild(tr);
	        });
        if (!cb.children.length) cb.innerHTML = '<tr><td colspan="5">No completed transfers found.</td></tr>';
      } catch (err) {
        console.error(err);
      }
    }

    resetTransferSummary();
    resetAIPDetails();
    qq('.tab-btn[data-tab]').forEach((btn) => {
      btn.addEventListener('click', () => switchTab(btn.dataset.tab));
    });
    const failureWindowSelect = q('#failure-window');
    const failureWindowFailedSelect = q('#failure-window-failed');
    if (failureWindowSelect) {
      failureWindow = failureWindowSelect.value || '24';
      failureWindowSelect.addEventListener('change', () => {
        failureWindow = failureWindowSelect.value || '24';
        if (failureWindowFailedSelect) failureWindowFailedSelect.value = failureWindow;
        load();
        if (q('#tab-failed')?.classList.contains('active')) {
          loadFailedTab();
        }
      });
    }
    if (failureWindowFailedSelect) {
      failureWindowFailedSelect.value = failureWindow;
      failureWindowFailedSelect.addEventListener('change', () => {
        failureWindow = failureWindowFailedSelect.value || '24';
        if (failureWindowSelect) failureWindowSelect.value = failureWindow;
        load();
        if (q('#tab-failed')?.classList.contains('active')) {
          loadFailedTab();
        }
      });
    }
    const reportFrom = q('#report-date-from');
    const reportTo = q('#report-date-to');
    if (reportFrom && reportTo) {
      const now = new Date();
      const toISO = now.toISOString().slice(0, 10);
      const fromDate = new Date(now.getTime() - (1000 * 60 * 60 * 24 * 30));
      const fromISO = fromDate.toISOString().slice(0, 10);
      reportFrom.value = fromISO;
      reportTo.value = toISO;
    }
    q('#report-run').addEventListener('click', () => runReportQuery());
    q('#report-export-csv').addEventListener('click', () => exportReportCSV());
    q('#report-save-template').addEventListener('click', () => saveReportTemplate());
    q('#report-load-template').addEventListener('click', () => loadSelectedTemplate());
    q('#report-delete-template').addEventListener('click', () => deleteSelectedTemplate());
    q('#aip-load-more').addEventListener('click', () => loadAIPList(false));
	    q('#failed-body').addEventListener('click', (ev) => {
	      const target = ev.target;
	      if (!(target instanceof Element)) return;
	      if (target.closest('a')) return;
	      const tr = target.closest('tr');
	      if (!tr) return;
	      const transferUUID = tr.getAttribute('data-transfer-uuid') || '';
	      if (!transferUUID) return;
	      selectRow('#failed-body', tr);
	      loadFailedTransferDetails(transferUUID, {
	        microservice_group: tr.getAttribute('data-microservice-group') || '',
	        error_text: tr.getAttribute('data-error-text') || '',
	      });
	    });
    q('#aip-largest-status-filter').addEventListener('change', renderLargestFiles);
    q('#aip-largest-ext-filter').addEventListener('input', renderLargestFiles);
    q('#aip-largest-sort').addEventListener('change', renderLargestFiles);
    q('#aip-export-format-versions').addEventListener('click', () => {
      const d = aipCurrentStats || {};
      const rows = (d.format_version_counts || []).map((it) => [
        d.aip_uuid || '',
        it.format_registry_key || '',
        it.format_name || '',
        it.format_version || '',
        it.count || 0,
      ]);
      downloadCSV('aip-format-versions-' + (d.aip_uuid || 'unknown') + '.csv', ['AIPUUID', 'FormatRegistryKey', 'FormatName', 'FormatVersion', 'Count'], rows);
    });
    q('#aip-export-largest-files').addEventListener('click', () => {
      const d = aipCurrentStats || {};
      const rows = (d.largest_files || []).map((it) => [
        d.aip_uuid || '',
        it.file_uuid || '',
        it.file_path || '',
        it.extension || '',
        it.bytes || 0,
        it.format_registry_key || '',
        it.format_name || '',
        it.status || '',
        it.created_by_app_date || '',
        it.indexed_at || '',
      ]);
      downloadCSV('aip-largest-files-' + (d.aip_uuid || 'unknown') + '.csv', ['AIPUUID', 'FileUUID', 'FilePath', 'Extension', 'Bytes', 'FormatRegistryKey', 'FormatName', 'Status', 'CreatedByAppDate', 'IndexedAt'], rows);
    });
    loadRiskThresholds().finally(() => load());
    setInterval(load, 15000);
    setInterval(loadServicesStatus, 30000);
  </script>
</body>
</html>`
