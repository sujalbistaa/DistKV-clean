import { useState } from "react";
import type { Snapshot } from "../api";

interface Props {
  snapshot: Snapshot;
  enabled: boolean;
  busy: boolean;
  error: string;
  onCrashLeader: () => void;
  onPartition: (group: string[]) => void;
  onHeal: () => void;
  onRecover: () => void;
}

/**
 * The faults a visitor can introduce. Each one is the same fault the test
 * suite injects through the fake network, applied to a real cluster: the
 * leader really does stop, the partitioned nodes really do stop hearing
 * from anyone, and what comes next is whatever the consensus code does
 * about it.
 */
export function ChaosPanel({
  snapshot,
  enabled,
  busy,
  error,
  onCrashLeader,
  onPartition,
  onHeal,
  onRecover,
}: Props) {
  const [group, setGroup] = useState<string[]>([]);
  const partitioned = snapshot.nodes.some((n) => n.isolatedFrom.length > 0);
  const anyDown = snapshot.nodes.some((n) => n.lifecycle !== "running");
  const hasLeader = snapshot.cluster.leader !== "";

  const toggle = (id: string) =>
    setGroup((current) =>
      current.includes(id) ? current.filter((n) => n !== id) : [...current, id],
    );

  const splitSize = group.length;
  const otherSide = snapshot.cluster.size - splitSize;
  const canSplit = splitSize > 0 && splitSize < snapshot.cluster.size;

  if (!enabled) {
    return (
      <section className="panel">
        <div className="panel-head">
          <h2 className="panel-title">Fault injection</h2>
        </div>
        <div className="panel-body">
          <p className="hint">Disabled on this deployment. The view above is live and read-only.</p>
        </div>
      </section>
    );
  }

  return (
    <section className="panel">
      <div className="panel-head">
        <h2 className="panel-title">Break it</h2>
        <span className="panel-note">nothing here can lose a committed write</span>
      </div>
      <div className="panel-body controls">
        <div className="control-group">
          <button
            className="btn btn-primary"
            disabled={busy || !hasLeader}
            onClick={onCrashLeader}
          >
            Crash the leader
          </button>
          <p className="hint">
            {hasLeader ? (
              <>
                Kills <span className="mono">{snapshot.cluster.leader}</span> outright — goroutines
                gone, files closed, disk untouched. The remaining four notice within 300&nbsp;ms and
                elect a replacement. Read a key afterwards: it is still there.
              </>
            ) : (
              "No leader at this instant — an election is running. It will resolve in a moment."
            )}
          </p>
        </div>

        <div className="control-group">
          <div className="label">Partition the network</div>
          <div className="btn-row">
            {snapshot.nodes.map((node) => (
              <button
                key={node.id}
                className={`btn btn-node${group.includes(node.id) ? " on" : ""}`}
                disabled={busy}
                onClick={() => toggle(node.id)}
              >
                {node.id}
              </button>
            ))}
          </div>
          <div className="btn-row">
            <button
              className="btn"
              disabled={busy || !canSplit}
              onClick={() => {
                onPartition(group);
                setGroup([]);
              }}
            >
              Cut the network
            </button>
            <button className="btn" disabled={busy || !partitioned} onClick={onHeal}>
              Heal
            </button>
          </div>
          <p className="hint">
            {canSplit ? (
              <>
                Splits the cluster {splitSize} against {otherSide}.{" "}
                {splitSize >= snapshot.cluster.quorum
                  ? "The side you selected keeps the majority and carries on serving; the other side cannot elect anyone and stalls."
                  : "The side you selected loses its majority: it will hold election after election, driving its term up, and win none of them."}
              </>
            ) : (
              "Select the nodes on one side of the split. Neither side will be able to hear the other."
            )}
          </p>
        </div>

        <div className="control-group">
          <button className="btn" disabled={busy || (!anyDown && !partitioned)} onClick={onRecover}>
            Restore everything
          </button>
          <p className="hint">
            Restarts every crashed node — each one replaying its own write-ahead log — and heals
            every partition. The cluster is also restored automatically if it is left broken and
            idle.
          </p>
        </div>

        {error && <div className="error-line">{error}</div>}
      </div>
    </section>
  );
}
