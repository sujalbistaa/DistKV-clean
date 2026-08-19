import { useState } from "react";
import { api, type Config, type OpResult } from "../api";

type Op = "get" | "put" | "append" | "delete";

/**
 * A key-value client with its work shown. The result panel is the point:
 * every node the request touched, who refused it and why, who redirected
 * it where, and the log index it finally committed at. Run it once with
 * the cluster healthy and once just after crashing the leader and the
 * difference is the entire failover story, in four lines.
 */
export function KvConsole({ config }: { config: Config | null }) {
  const [key, setKey] = useState("city");
  const [value, setValue] = useState("kathmandu");
  const [result, setResult] = useState<OpResult | null>(null);
  const [error, setError] = useState("");
  const [pending, setPending] = useState<Op | null>(null);

  const run = async (op: Op) => {
    setPending(op);
    setError("");
    try {
      setResult(await api.kv(op, key, value));
    } catch (err) {
      setResult(null);
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setPending(null);
    }
  };

  const busy = pending !== null;

  return (
    <section className="panel">
      <div className="panel-head">
        <h2 className="panel-title">Client</h2>
        <span className="panel-note">
          every operation goes through the log, reads included
        </span>
      </div>
      <div className="panel-body">
        <div className="form-row">
          <label className="field">
            <span className="label">Key</span>
            <input
              type="text"
              value={key}
              maxLength={config?.maxKeyLen ?? 64}
              onChange={(e) => setKey(e.target.value)}
              placeholder="city"
            />
          </label>
          <label className="field">
            <span className="label">Value</span>
            <input
              type="text"
              value={value}
              maxLength={config?.maxValueLen ?? 512}
              onChange={(e) => setValue(e.target.value)}
              placeholder="kathmandu"
            />
          </label>
        </div>

        <div className="btn-row" style={{ marginTop: 12 }}>
          <button className="btn btn-primary" disabled={busy || !key} onClick={() => run("put")}>
            {pending === "put" ? "Committing…" : "Put"}
          </button>
          <button className="btn" disabled={busy || !key} onClick={() => run("get")}>
            {pending === "get" ? "Reading…" : "Get"}
          </button>
          <button className="btn" disabled={busy || !key} onClick={() => run("append")}>
            Append
          </button>
          <button className="btn btn-danger" disabled={busy || !key} onClick={() => run("delete")}>
            Delete
          </button>
        </div>

        {error && <div className="error-line">{error}</div>}
        {result && <Trace result={result} />}
      </div>
    </section>
  );
}

function Trace({ result }: { result: OpResult }) {
  return (
    <>
      {result.ok && result.op === "get" && (
        <div className="result-line">
          {result.found ? (
            <>
              {result.key} = {JSON.stringify(result.value)}
            </>
          ) : (
            <span style={{ color: "var(--ink-3)" }}>{result.key} — no such key</span>
          )}
        </div>
      )}
      {!result.ok && result.error && <div className="error-line">{result.error}</div>}

      <div className="trace">
        <div className={`trace-head${result.ok ? "" : " failed"}`}>
          <span>
            {result.ok ? (
              <>
                {result.op} committed at log index <strong className="mono">{result.index}</strong>{" "}
                by <strong className="mono">{result.servedBy}</strong>
              </>
            ) : (
              <>{result.op} did not commit</>
            )}
          </span>
          <span className="mono">{result.totalMs.toFixed(1)} ms</span>
        </div>
        {result.attempts.map((hop, i) => (
          <div className={`hop ${hop.outcome}`} key={`${hop.node}-${i}`}>
            <span className="hop-index">{i + 1}</span>
            <span className="hop-node">{hop.node}</span>
            <span className="hop-detail">
              <span className="outcome">{label(hop.outcome)}</span>
              {hop.detail ? ` — ${hop.detail}` : ""}
            </span>
            <span className="hop-time">{hop.latencyMs.toFixed(1)} ms</span>
          </div>
        ))}
      </div>
    </>
  );
}

function label(outcome: string): string {
  switch (outcome) {
    case "ok":
      return "served";
    case "redirected":
      return "redirected";
    case "refused":
      return "refused";
    case "unreachable":
      return "no answer";
    default:
      return outcome;
  }
}
