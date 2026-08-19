import type { NodeView, Snapshot } from "../api";

interface Props {
  snapshot: Snapshot;
  busy: boolean;
  onCrash: (node: string) => void;
  onRestart: (node: string) => void;
}

/**
 * One row per node, in a fixed order, with the numbers that change
 * fastest on the right where the eye can scan a column of them. The row
 * itself carries the state: a leading node is the only blue thing on the
 * page, a crashed one the only red.
 */
export function NodeTable({ snapshot, busy, onCrash, onRestart }: Props) {
  // The leader is the one a quorum agrees on, not merely a node calling
  // itself leader: one that has been partitioned away goes on believing it
  // leads until it hears a higher term, and it would otherwise take the
  // emphasis from the node actually serving the cluster. Its own row still
  // says "leader", which is exactly what it still thinks.
  const leader =
    snapshot.nodes.find((n) => n.id === snapshot.cluster.leader) ??
    snapshot.nodes.find((n) => n.role === "leader" && n.lifecycle === "running");
  const target = leader?.lastLogIndex ?? 0;

  return (
    <section className="panel">
      <div className="panel-head">
        <h2 className="panel-title">Nodes</h2>
        <span className="panel-note">
          each one a Raft peer with its own write-ahead log and snapshots
        </span>
      </div>
      <div className="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Node</th>
              <th>Role</th>
              <th className="num">Term</th>
              <th className="num">Commit</th>
              <th className="num">Last log</th>
              <th>Replicated</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {snapshot.nodes.map((node) => (
              <Row
                key={node.id}
                node={node}
                leaderTarget={target}
                isLeader={node.id === leader?.id}
                busy={busy}
                onCrash={onCrash}
                onRestart={onRestart}
              />
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function Row({
  node,
  leaderTarget,
  isLeader,
  busy,
  onCrash,
  onRestart,
}: {
  node: NodeView;
  leaderTarget: number;
  isLeader: boolean;
  busy: boolean;
  onCrash: (node: string) => void;
  onRestart: (node: string) => void;
}) {
  const down = node.lifecycle !== "running";
  const cut = node.isolatedFrom.length > 0;

  const rowClass = [
    isLeader ? "is-leader" : "",
    down ? "is-down" : cut ? "is-cut" : "",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <tr className={rowClass}>
      <td>
        <div className="node-id">{node.id}</div>
        <div className="node-addr">{node.addr}</div>
      </td>
      <td>
        <Role node={node} />
        {cut && !down && <span className="flag">{isolationText(node.isolatedFrom)}</span>}
      </td>
      <td className="num">{down ? "—" : node.term}</td>
      <td className="num">{down ? "—" : node.commitIndex}</td>
      <td className="num">{down ? "—" : node.lastLogIndex}</td>
      <td>
        <Replication node={node} leaderTarget={leaderTarget} isLeader={isLeader} />
      </td>
      <td style={{ textAlign: "right" }}>
        {down ? (
          <button className="btn btn-small" disabled={busy} onClick={() => onRestart(node.id)}>
            Restart
          </button>
        ) : (
          <button
            className="btn btn-small btn-danger"
            disabled={busy}
            onClick={() => onCrash(node.id)}
          >
            Crash
          </button>
        )}
      </td>
    </tr>
  );
}

/**
 * Naming the peers is useful when a node is cut off from one or two of
 * them and useless when it is cut off from four: the list stops being read
 * and starts being a paragraph inside a table cell.
 */
function isolationText(peers: string[]): string {
  if (peers.length <= 2) return `cut off from ${peers.join(" and ")}`;
  return `cut off from ${peers.length} peers`;
}

function Role({ node }: { node: NodeView }) {
  if (node.lifecycle === "crashed") {
    return (
      <span className="role down">
        <span className="dot" />
        crashed
      </span>
    );
  }
  if (node.lifecycle === "unreachable") {
    return (
      <span className="role down">
        <span className="dot" />
        unreachable
      </span>
    );
  }
  return (
    <span className={`role ${node.role}`}>
      <span className="dot" />
      {node.role}
    </span>
  );
}

/**
 * How far this node has been driven towards the leader's last log entry.
 * The leader's own row shows nothing — it is the target, and a bar that is
 * always full says nothing worth the ink.
 */
function Replication({
  node,
  leaderTarget,
  isLeader,
}: {
  node: NodeView;
  leaderTarget: number;
  isLeader: boolean;
}) {
  if (node.lifecycle !== "running" || isLeader || leaderTarget === 0) {
    return <span className="repl-text">—</span>;
  }

  const behind = Math.max(0, leaderTarget - node.lastLogIndex);
  const percent = Math.min(100, (node.lastLogIndex / leaderTarget) * 100);

  return (
    <div className="repl">
      <div className="repl-track">
        <div
          className={`repl-fill${behind > 0 ? " behind" : ""}`}
          style={{ width: `${percent}%` }}
        />
      </div>
      <span className="repl-text">{behind === 0 ? "caught up" : `−${behind}`}</span>
    </div>
  );
}
