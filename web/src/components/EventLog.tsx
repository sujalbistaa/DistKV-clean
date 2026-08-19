import type { ClusterEvent } from "../api";

/**
 * What the cluster did, newest first, derived by the gateway from
 * consecutive polls rather than from anything the nodes announce. Elections
 * and lost quorums come out of watching state change; writes are reported
 * by the handler that performed them.
 */
export function EventLog({ events }: { events: ClusterEvent[] }) {
  const newestFirst = [...events].reverse();

  return (
    <section className="panel">
      <div className="panel-head">
        <h2 className="panel-title">Cluster log</h2>
        <span className="panel-note">{events.length} events</span>
      </div>
      <div className="log">
        {newestFirst.length === 0 ? (
          <p className="log-empty">
            Nothing has happened yet. Write a key, or crash the leader, and it will show up here.
          </p>
        ) : (
          newestFirst.map((event) => (
            <div className={`log-row ${event.kind}`} key={event.seq}>
              <span className="log-time">{clock(event.ts)}</span>
              <span>{event.text}</span>
            </div>
          ))
        )}
      </div>
    </section>
  );
}

function clock(ts: number): string {
  const d = new Date(ts);
  return [d.getHours(), d.getMinutes(), d.getSeconds()]
    .map((n) => String(n).padStart(2, "0"))
    .join(":");
}
