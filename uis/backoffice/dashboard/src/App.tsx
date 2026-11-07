import { LogoutForm } from "./components/AuthForms";

type Metric = {
  label: string;
  value: string;
  delta: string;
  tone: "positive" | "warning" | "critical";
};

type DepartmentPanel = {
  title: string;
  badge: string;
  badgeClass: "positive" | "warning" | "critical";
  metricLabel: string;
  metricValue: string;
  progress: number;
  alerts: string[];
};

export type DashboardData = {
  metrics: Metric[];
  panels: DepartmentPanel[];
};

type AppProps = {
  data?: DashboardData;
  error?: string;
};

function App({ data, error }: AppProps) {
  const { metrics, panels } = data ?? { metrics: [], panels: [] };
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
          <span className="navLabel">Overview</span>
          <a className="active" href="#">
            Dashboard
          </a>
          <a href="/telemetry">Technical telemetry</a>
          <a href="/reporting">Support performance</a>
          <a href="#">Sales</a>
          <a href="#">Training</a>
          <a href="#">HR</a>
        </nav>

        <div className="statusCard">
          <span className="statusDot" />
          <div>
            <strong>Operations status</strong>
            <small>{error ? "Unavailable" : "Needs attention"}</small>
          </div>
        </div>
        <LogoutForm />
      </aside>

      <main className="main">
        <header className="topbar">
          <div>
            <div className="eyebrow">Executive view</div>
            <h1>Performance dashboard</h1>
          </div>
          <button className="ctaButton">Generate report</button>
        </header>

        {error && <p role="alert">{error}</p>}

        <section className="metricsGrid">
          {metrics.map((metric) => (
            <article key={metric.label} className="metricCard">
              <div className="metricHeader">
                <span>{metric.label}</span>
                <span className={`metricTone ${metric.tone}`}>
                  {metric.delta}
                </span>
              </div>
              <div className="metricValue">{metric.value}</div>
            </article>
          ))}
        </section>

        <section className="contentGrid">
          {panels.map((panel) => (
            <article key={panel.title} className="panel">
              <div className="panelHeader">
                <h2>{panel.title}</h2>
                <span className={`chip ${panel.badgeClass}`}>
                  {panel.badge}
                </span>
              </div>

              <div className="progressBlock">
                <div className="progressMeta">
                  <span>{panel.metricLabel}</span>
                  <strong>{panel.metricValue}</strong>
                </div>
                <div className="progressTrack">
                  <div
                    className={`progressFill ${panel.badgeClass}`}
                    style={{ width: `${panel.progress}%` }}
                  />
                </div>
              </div>

              <ul className="alertList">
                {panel.alerts.map((alert) => (
                  <li key={alert}>{alert}</li>
                ))}
              </ul>
            </article>
          ))}
        </section>
      </main>
    </div>
  );
}

export default App;
