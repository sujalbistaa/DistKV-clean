import { useCallback, useEffect, useRef, useState } from "react";
import { api, subscribe, type ClusterEvent, type Config, type Snapshot } from "./api";
import { StatStrip } from "./components/StatStrip";
import { NodeTable } from "./components/NodeTable";
import { ChaosPanel } from "./components/ChaosPanel";
import { KvConsole } from "./components/KvConsole";
import { EventLog } from "./components/EventLog";

const GITHUB = "https://github.com/sujalbistaa/DistKV";
const EVENT_LIMIT = 200;

export default function App() {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [events, setEvents] = useState<ClusterEvent[]>([]);
  const [config, setConfig] = useState<Config | null>(null);
  const [connected, setConnected] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  // Events arrive in batches that can overlap after a reconnection, so
  // they are merged on sequence number rather than appended.
  const seen = useRef(new Set<number>());

  useEffect(() => {
    api.config().then(setConfig).catch(() => undefined);

    return subscribe({
      onSnapshot: setSnapshot,
      onStatus: setConnected,
      onEvents: (batch) => {
        const fresh = batch.filter((e) => !seen.current.has(e.seq));
        if (fresh.length === 0) return;
        fresh.forEach((e) => seen.current.add(e.seq));
        setEvents((current) => [...current, ...fresh].slice(-EVENT_LIMIT));
      },
    });
  }, []);

  // Faults are serialised behind one busy flag: two of them in flight at
  // once would make the cluster's response impossible to attribute, which
  // defeats the point of watching.
  const runFault = useCallback(async (fault: () => Promise<unknown>) => {
    setBusy(true);
    setError("");
    try {
      await fault();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setBusy(false);
    }
  }, []);

  if (!snapshot) {
    return (
      <>
        <Masthead connected={connected} snapshot={null} />
        <main className="shell">
          <p className="hint" style={{ marginTop: 28 }}>
            {connected ? "Waiting for the first poll…" : "Connecting to the cluster…"}
          </p>
        </main>
      </>
    );
  }

  return (
    <>
      <Masthead connected={connected} snapshot={snapshot} />
      <main className="shell">
        <div className="stack" style={{ marginTop: 20 }}>
          <StatStrip cluster={snapshot.cluster} />

          <p className="explainer">
            Five processes, each running an implementation of the Raft consensus protocol written
            from scratch — no <span className="mono">hashicorp/raft</span>, no consensus library of
            any kind. They elect a leader among themselves, replicate an append-only log, and apply
            it to a key-value store. <strong>Every control on this page acts on that live cluster.</strong>{" "}
            Crash the leader and watch the election; cut the network and watch the minority fail to
            win one.
          </p>

          <div className="columns">
            <div className="stack">
              <NodeTable
                snapshot={snapshot}
                busy={busy}
                onCrash={(node) => runFault(() => api.crash(node))}
                onRestart={(node) => runFault(() => api.restart(node))}
              />
              <KvConsole config={config} />
            </div>

            <div className="stack">
              <ChaosPanel
                snapshot={snapshot}
                enabled={config?.allowChaos ?? true}
                busy={busy}
                error={error}
                onCrashLeader={() => runFault(api.crashLeader)}
                onPartition={(group) => runFault(() => api.partition(group))}
                onHeal={() => runFault(api.heal)}
                onRecover={() => runFault(api.recover)}
              />
              <EventLog events={events} />
            </div>
          </div>

          <footer className="colophon">
            <span>
              A fault-injection soak test ran 576,528 operations against this code under{" "}
              <span className="mono">-race</span> while a chaos loop partitioned, crashed, and
              restarted nodes throughout — every history checked for linearizability, zero
              violations.
            </span>
            <a href={GITHUB}>Read the source</a>
          </footer>
        </div>
      </main>
    </>
  );
}

function Masthead({ connected, snapshot }: { connected: boolean; snapshot: Snapshot | null }) {
  return (
    <header className="masthead">
      <div className="masthead-inner">
        <div>
          <h1 className="wordmark">DistKV — cluster console</h1>
          <p className="tagline">
            A distributed, fault-tolerant key-value store on a Raft implementation written from
            scratch in Go. This page is attached to a running five-node cluster.
          </p>
        </div>
        <div className="masthead-meta">
          {snapshot && (
            <span className="mono">
              {snapshot.cluster.running}/{snapshot.cluster.size} up
            </span>
          )}
          <span className={`conn${connected ? "" : " offline"}`}>
            <span className="dot" />
            {connected ? "live" : "reconnecting"}
          </span>
        </div>
      </div>
    </header>
  );
}
