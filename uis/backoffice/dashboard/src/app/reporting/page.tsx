import type { Metadata } from "next";
import { redirect } from "next/navigation";
import { LogoutForm } from "../../components/AuthForms";
import { getSession, getSessionToken } from "../../lib/auth";

type RateKpi = {
  numerator: number;
  denominator: number;
  rate_percent: number | null;
  status: string;
};

type SupportPerformanceReport = {
  period_start: string;
  period_end: string;
  period_timezone: string;
  generated_at: string;
  sla_attainment: RateKpi;
  average_resolution_time: {
    sum_seconds: number;
    sample_count: number;
    average_seconds: number | null;
    average_hours: number | null;
    status: string;
  };
  eligible_query_automation_resolution: RateKpi;
  queues: {
    queue_id: string;
    open_ticket_count: number | null;
    overdue_ticket_count: number | null;
    unassigned_ticket_count: number | null;
    active_agent_count: number | null;
    oldest_open_age_seconds: number | null;
    snapshot_at: string | null;
    snapshot_age_seconds: number | null;
    freshness_status: string;
  }[];
};

type ReportState = {
  report?: SupportPerformanceReport;
  message?: string;
  unavailable?: boolean;
};

export const metadata: Metadata = {
  title: "Customer Support performance | Nexova Backoffice",
};

function formatDate(value: string, timeZone: string): string {
  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    timeZone,
  }).format(new Date(value));
}

function formatDateTime(value: string, timeZone: string): string {
  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    timeZone,
  }).format(new Date(value));
}

function formatCount(value: number | null): string {
  return value === null
    ? "No data"
    : new Intl.NumberFormat("en-GB").format(value);
}

function formatAge(value: number | null): string {
  if (value === null) return "No data";
  const totalMinutes = Math.floor(value / 60);
  const days = Math.floor(totalMinutes / 1440);
  const hours = Math.floor((totalMinutes % 1440) / 60);
  const minutes = totalMinutes % 60;
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

function rateLabel(metric: RateKpi): string {
  if (metric.status === "no_data" || metric.rate_percent === null)
    return "No data";
  return `${metric.rate_percent.toFixed(1)}%`;
}

async function getSupportReport(token: string): Promise<ReportState> {
  const apiUrl = process.env.REPORTING_API_URL ?? "http://127.0.0.1:8005";
  try {
    const response = await fetch(
      `${apiUrl.replace(/\/$/, "")}/reporting/kpis`,
      {
        headers: { Authorization: `Bearer ${token}` },
        cache: "no-store",
        signal: AbortSignal.timeout(5000),
      },
    );

    if (response.status === 404) {
      return { message: "No weekly support report has been published yet." };
    }
    if (response.status === 401 || response.status === 403) {
      return {
        message:
          "Support reporting access is unavailable. Contact your administrator.",
        unavailable: true,
      };
    }
    if (!response.ok) throw new Error("Reporting API request failed");
    return { report: (await response.json()) as SupportPerformanceReport };
  } catch {
    return {
      message:
        "The weekly support report is currently unavailable. Try again later.",
      unavailable: true,
    };
  }
}

export default async function ReportingPage() {
  const session = await getSession();
  if (!session) redirect("/login");

  const token = await getSessionToken();
  if (!token) redirect("/login");
  const { report, message, unavailable } = await getSupportReport(token);

  return (
    <div className="shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brandMark">N</div>
          <div>
            <div className="brandTitle">Nexova</div>
            <div className="brandSubtitle">Operations</div>
          </div>
        </div>

        <nav className="nav" aria-label="Dashboard navigation">
          <span className="navLabel">Customer Support</span>
          <a href="/">Overview</a>
          <a className="active" href="/reporting" aria-current="page">
            Weekly performance
          </a>
          <a href="/telemetry">Technical telemetry</a>
        </nav>

        <div className="statusCard">
          <span
            className={`statusDot${unavailable ? " telemetryOffline" : " telemetryOnline"}`}
          />
          <div>
            <strong>Support report</strong>
            <small>
              {unavailable
                ? "Unavailable"
                : report
                  ? "Published"
                  : "Not published"}
            </small>
          </div>
        </div>
        <LogoutForm />
      </aside>

      <main className="main reportingMain">
        <header className="topbar">
          <div>
            <div className="eyebrow">Customer Support</div>
            <h1>Weekly performance</h1>
          </div>
          <a className="telemetryRefresh" href="/reporting">
            Refresh report
          </a>
        </header>

        {message && (
          <p
            className={`reportingMessage${unavailable ? " reportingUnavailable" : ""}`}
            role={unavailable ? "alert" : "status"}
          >
            {message}
          </p>
        )}

        {report && (
          <>
            <section className="reportingPeriod" aria-label="Reporting period">
              <div>
                <span className="eyebrow">
                  Reporting week · {report.period_timezone}
                </span>
                <p>
                  <time dateTime={report.period_start}>
                    {formatDate(report.period_start, report.period_timezone)}
                  </time>
                  <span aria-hidden="true"> to </span>
                  <time dateTime={report.period_end}>
                    {formatDate(report.period_end, report.period_timezone)}
                  </time>
                  <span className="reportingPeriodNote"> (end exclusive)</span>
                </p>
              </div>
              <p className="reportingGenerated">
                Published{" "}
                {formatDateTime(report.generated_at, report.period_timezone)}
              </p>
            </section>

            <section
              className="reportingMetrics"
              aria-label="Support performance measures"
            >
              <article className="reportingMetric" aria-labelledby="sla-title">
                <div>
                  <span className="eyebrow">Service level</span>
                  <h2 id="sla-title">24-hour SLA attainment</h2>
                </div>
                <div className="reportingMetricResult">
                  <strong>{rateLabel(report.sla_attainment)}</strong>
                  <span>
                    {report.sla_attainment.numerator} of{" "}
                    {report.sla_attainment.denominator} resolved tickets met SLA
                  </span>
                </div>
                <p className="reportingContext">
                  Commitment: resolution within 24 hours
                </p>
              </article>

              <article
                className="reportingMetric"
                aria-labelledby="resolution-title"
              >
                <div>
                  <span className="eyebrow">Resolution</span>
                  <h2 id="resolution-title">Average resolution time</h2>
                </div>
                <div className="reportingMetricResult">
                  <strong>
                    {report.average_resolution_time.status === "no_data" ||
                    report.average_resolution_time.average_hours === null
                      ? "No data"
                      : `${report.average_resolution_time.average_hours.toFixed(1)} hours`}
                  </strong>
                  <span>
                    {report.average_resolution_time.sample_count} valid resolved
                    tickets
                  </span>
                </div>
                <p className="reportingContext">Briefing baseline: 48 hours</p>
              </article>

              <article
                className="reportingMetric"
                aria-labelledby="automation-title"
              >
                <div>
                  <span className="eyebrow">Automation</span>
                  <h2 id="automation-title">
                    Eligible-query automation resolution
                  </h2>
                </div>
                <div className="reportingMetricResult">
                  <strong>
                    {rateLabel(report.eligible_query_automation_resolution)}
                  </strong>
                  <span>
                    {report.eligible_query_automation_resolution.numerator} of{" "}
                    {report.eligible_query_automation_resolution.denominator}{" "}
                    eligible queries resolved by bot
                  </span>
                </div>
                <p className="reportingContext">
                  Target: 40% resolved without human intervention
                </p>
              </article>
            </section>

            <section
              className="reportingQueueSection"
              aria-labelledby="backlog-title"
            >
              <header className="reportingSectionHeader">
                <div>
                  <span className="eyebrow">Week-end snapshot</span>
                  <h2 id="backlog-title">Week-end support backlog</h2>
                </div>
                <span className="reportingContext">
                  Point-in-time values by queue
                </span>
              </header>
              <div className="reportingTableWrap">
                <table className="reportingTable">
                  <thead>
                    <tr>
                      <th scope="col">Queue</th>
                      <th scope="col">Open tickets</th>
                      <th scope="col">Overdue</th>
                      <th scope="col">Unassigned</th>
                      <th scope="col">Active agents</th>
                      <th scope="col">Oldest open age</th>
                      <th scope="col">Snapshot at</th>
                      <th scope="col">Freshness</th>
                    </tr>
                  </thead>
                  <tbody>
                    {report.queues.map((queue) => (
                      <tr key={queue.queue_id}>
                        <th scope="row">{queue.queue_id}</th>
                        <td>{formatCount(queue.open_ticket_count)}</td>
                        <td>{formatCount(queue.overdue_ticket_count)}</td>
                        <td>{formatCount(queue.unassigned_ticket_count)}</td>
                        <td>{formatCount(queue.active_agent_count)}</td>
                        <td>{formatAge(queue.oldest_open_age_seconds)}</td>
                        <td>
                          {queue.snapshot_at
                            ? formatDateTime(
                                queue.snapshot_at,
                                report.period_timezone,
                              )
                            : "No data"}
                        </td>
                        <td>
                          <span
                            className={`reportingFreshness reportingFreshness-${queue.freshness_status}`}
                          >
                            {queue.freshness_status}
                          </span>
                          {queue.snapshot_age_seconds !== null && (
                            <span className="reportingSnapshotAge">
                              {formatAge(queue.snapshot_age_seconds)} old
                            </span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {report.queues.length === 0 && (
                  <p className="reportingEmpty">
                    No queue snapshots are available for this reporting week.
                  </p>
                )}
              </div>
            </section>
          </>
        )}
      </main>
    </div>
  );
}
