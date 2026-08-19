import type { ClusterView } from "../api";

/**
 * The five numbers worth having permanently on screen. Quorum is the one
 * that matters: when it goes, the store stops accepting writes, and the
 * strip should say so in the same place it always says everything else
 * rather than by producing a new alert somewhere.
 */
export function StatStrip({ cluster }: { cluster: ClusterView }) {
  const electing = cluster.leader === "";
  const racing = cluster.highestTerm > cluster.term && cluster.term > 0;

  return (
    <div className="strip">
      <div className="stat">
        <div className="label">Leader</div>
        <div className="stat-value">{cluster.leader || "—"}</div>
        <div className="stat-sub">
          {electing ? "election in progress" : `elected by ${cluster.quorum} of ${cluster.size}`}
        </div>
      </div>

      <div className="stat">
        <div className="label">Term</div>
        <div className="stat-value">{cluster.term || cluster.highestTerm}</div>
        <div className="stat-sub">
          {racing ? `cut-off nodes at term ${cluster.highestTerm}` : "elections so far"}
        </div>
      </div>

      <div className={`stat${cluster.hasQuorum ? "" : " is-warning"}`}>
        <div className="label">Connected</div>
        <div className="stat-value">{cluster.largestGroup}</div>
        <div className="stat-sub">
          {cluster.groups > 1 && `split ${cluster.groups} ways · `}
          {cluster.hasQuorum
            ? `${cluster.quorum} needed — writes accepted`
            : `${cluster.quorum} needed — writes refused`}
        </div>
      </div>

      <div className="stat">
        <div className="label">Committed</div>
        <div className="stat-value">{cluster.commitIndex}</div>
        <div className="stat-sub">log entries agreed by a majority</div>
      </div>

      <div className="stat">
        <div className="label">Keys</div>
        <div className="stat-value">{cluster.keyCount}</div>
        <div className="stat-sub">in the replicated state machine</div>
      </div>
    </div>
  );
}
