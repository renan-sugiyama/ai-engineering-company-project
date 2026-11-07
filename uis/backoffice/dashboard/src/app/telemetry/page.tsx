import { randomUUID } from "node:crypto";
import { redirect } from "next/navigation";
import { LogoutForm } from "../../components/AuthForms";
import { TelemetrySession } from "../../components/TelemetrySession";
import { getSession } from "../../lib/auth";

type TelemetryReport = {
  period: { from: string; to: string };
  metrics: {
    events_per_day: {
      date: string;
      event_type: string;
      count: number;
    }[];
    error_rate_by_type: {
      date: string;
      request_type: string;
      request_count: number;
      server_error_count: number;
      error_rate: number;
    }[];
    average_latency_by_endpoint: {
      date: string;
      routeTemplate: string;
      method: string;
      sample_count: number;
      average_latency_ms: number;
    }[];
  };
};

export const metadata = { title: "Technical telemetry | Nexova Backoffice" };

export default async function TelemetryPage() {
  const session = await getSession();
  if (!session) redirect("/login");

  const requestId = randomUUID();
  const telemetrySession = (
    <TelemetrySession
      context={{
        userId: session.userId,
        sessionId: session.sessionId,
        expiresAt: session.expiresAt,
        requestId,
      }}
    />
  );

  let report: TelemetryReport | undefined;
  let error: string | undefined;

  try {
    const apiUrl = process.env.TELEMETRY_API_URL ?? "http://127.0.0.1:8000";
    const response = await fetch(
      `${apiUrl.replace(/\/$/, "")}/telemetry/report`,
      { cache: "no-store", signal: AbortSignal.timeout(5000) },
    );
    if (!response.ok) {
      throw new Error(`Telemetry API returned ${response.status}`);
    }
    report = (await response.json()) as TelemetryReport;
  } catch {
    error =
      "Telemetry report is unavailable. Check the telemetry API and database connection, then retry.";
  }

  return (
    <>
      {telemetrySession}
      <div className="shell">
        <aside className="sidebar">
          <div className="brand">
            <div className="brandMark">N</div>
            <div>
              <div className="brandTitle">Nexova</div>
              <div className="brandSubtitle">Engineering operations</div>
            </div>
          </div>

          <nav className="nav" aria-label="Dashboard navigation">
            <span className="navLabel">System</span>
            <a href="/">Overview</a>
            <a href="/reporting">Support performance</a>
            <a className="active" href="/telemetry" aria-current="page">
              Telemetry report
            </a>
          </nav>

          <div className="statusCard">
            <span
              className={`statusDot${error ? " telemetryOffline" : " telemetryOnline"}`}
            />
            <div>
              <strong>Telemetry API</strong>
              <small>{error ? "Unavailable" : "Report loaded"}</small>
            </div>
          </div>
          <LogoutForm />
        </aside>

        <main className="main telemetryMain">
          <header className="topbar">
            <div>
              <div className="eyebrow">Engineering / Operations</div>
              <h1>Technical telemetry</h1>
            </div>
            <a className="telemetryRefresh" href="/telemetry">
              Refresh report
            </a>
          </header>

          {error && (
            <p className="telemetryError" role="alert">
              {error}
            </p>
          )}

          {report && (
            <>
              <section className="telemetryPeriod" aria-label="Report period">
                <span className="eyebrow">Reporting window · UTC</span>
                <div>
                  <time dateTime={report.period.from}>
                    {report.period.from}
                  </time>
                  <span aria-hidden="true"> to </span>
                  <time dateTime={report.period.to}>{report.period.to}</time>
                </div>
              </section>

              <section
                className="telemetrySection"
                aria-labelledby="events-title"
              >
                <header className="telemetrySectionHeader">
                  <div>
                    <span className="eyebrow">Activity</span>
                    <h2 id="events-title">Events per day</h2>
                  </div>
                  <span className="telemetryTotal">
                    {report.metrics.events_per_day.reduce(
                      (total, row) => total + row.count,
                      0,
                    )}{" "}
                    events
                  </span>
                </header>
                <div className="telemetryTableWrap">
                  <table className="telemetryTable">
                    <thead>
                      <tr>
                        <th>Date (UTC)</th>
                        <th>Event type</th>
                        <th>Count</th>
                      </tr>
                    </thead>
                    <tbody>
                      {report.metrics.events_per_day.map((row) => (
                        <tr key={`${row.date}-${row.event_type}`}>
                          <td>{row.date}</td>
                          <td>{row.event_type}</td>
                          <td>{row.count}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {report.metrics.events_per_day.length === 0 && (
                    <p className="telemetryEmpty">
                      No operational events in this period.
                    </p>
                  )}
                </div>
              </section>

              <section
                className="telemetrySection"
                aria-labelledby="errors-title"
              >
                <header className="telemetrySectionHeader">
                  <div>
                    <span className="eyebrow">Reliability</span>
                    <h2 id="errors-title">HTTP 5xx rate by request type</h2>
                  </div>
                </header>
                <div className="telemetryTableWrap">
                  <table className="telemetryTable">
                    <thead>
                      <tr>
                        <th>Date (UTC)</th>
                        <th>Request type</th>
                        <th>5xx</th>
                        <th>Requests</th>
                        <th>Error rate</th>
                      </tr>
                    </thead>
                    <tbody>
                      {report.metrics.error_rate_by_type.map((row) => (
                        <tr key={`${row.date}-${row.request_type}`}>
                          <td>{row.date}</td>
                          <td>{row.request_type}</td>
                          <td>{row.server_error_count}</td>
                          <td>{row.request_count}</td>
                          <td>{(row.error_rate * 100).toFixed(1)}%</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {report.metrics.error_rate_by_type.length === 0 && (
                    <p className="telemetryEmpty">
                      No valid API request measurements in this period.
                    </p>
                  )}
                </div>
              </section>

              <section
                className="telemetrySection"
                aria-labelledby="latency-title"
              >
                <header className="telemetrySectionHeader">
                  <div>
                    <span className="eyebrow">Performance</span>
                    <h2 id="latency-title">Average API latency</h2>
                  </div>
                </header>
                <div className="telemetryTableWrap">
                  <table className="telemetryTable">
                    <thead>
                      <tr>
                        <th>Date (UTC)</th>
                        <th>Route</th>
                        <th>Method</th>
                        <th>Samples</th>
                        <th>Mean latency</th>
                      </tr>
                    </thead>
                    <tbody>
                      {report.metrics.average_latency_by_endpoint.map((row) => (
                        <tr
                          key={`${row.date}-${row.method}-${row.routeTemplate}`}
                        >
                          <td>{row.date}</td>
                          <td>{row.routeTemplate}</td>
                          <td>{row.method}</td>
                          <td>{row.sample_count}</td>
                          <td>{row.average_latency_ms.toFixed(1)} ms</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {report.metrics.average_latency_by_endpoint.length === 0 && (
                    <p className="telemetryEmpty">
                      No valid latency measurements in this period.
                    </p>
                  )}
                </div>
              </section>
            </>
          )}
        </main>
      </div>
    </>
  );
}
