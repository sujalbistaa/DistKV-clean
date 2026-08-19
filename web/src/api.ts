// The gateway's wire types and the two ways the console talks to it: a
// server-sent-events stream for the cluster view, and plain POSTs for
// everything that changes something.

export type Lifecycle = "running" | "crashed" | "unreachable";

export interface FollowerProgress {
  nextIndex: number;
  matchIndex: number;
}

export interface NodeView {
  id: string;
  addr: string;
  reachable: boolean;
  lifecycle: Lifecycle;
  error?: string;
  role: string;
  term: number;
  leaderId: string;
  commitIndex: number;
  lastApplied: number;
  lastLogIndex: number;
  logLen: number;
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  keyCount: number;
  uptimeMs: number;
  isolatedFrom: string[];
  followers?: Record<string, FollowerProgress>;
}

export interface ClusterView {
  size: number;
  quorum: number;
  running: number;
  leader: string;
  term: number;
  highestTerm: number;
  groups: number;
  largestGroup: number;
  hasQuorum: boolean;
  commitIndex: number;
  keyCount: number;
}

export interface Snapshot {
  ts: number;
  cluster: ClusterView;
  nodes: NodeView[];
}

export interface ClusterEvent {
  seq: number;
  ts: number;
  kind: "election" | "term" | "lifecycle" | "network" | "quorum" | "write";
  node?: string;
  text: string;
}

export interface Attempt {
  node: string;
  outcome: "ok" | "redirected" | "refused" | "unreachable";
  detail?: string;
  latencyMs: number;
}

export interface OpResult {
  ok: boolean;
  op: string;
  key: string;
  value: string;
  found: boolean;
  servedBy?: string;
  index?: number;
  attempts: Attempt[];
  totalMs: number;
  error?: string;
}

export interface Config {
  allowChaos: boolean;
  maxKeyLen: number;
  maxValueLen: number;
  maxKeys: number;
  quorum: number;
  size: number;
}

async function post<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body ?? {}),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    throw new Error((data as { error?: string }).error ?? `request failed (${res.status})`);
  }
  return data as T;
}

export const api = {
  config: async (): Promise<Config> => {
    const res = await fetch("/api/config");
    if (!res.ok) throw new Error("could not read gateway configuration");
    return res.json();
  },

  kv: (op: "get" | "put" | "append" | "delete", key: string, value = "") =>
    post<OpResult>(`/api/kv/${op}`, { key, value }),

  // Every fault goes to the same endpoint shape, so a caller names the
  // fault rather than assembling a request.
  crash: (node: string) => post("/api/chaos/crash", { node }),
  crashLeader: () => post<{ node: string }>("/api/chaos/crash-leader", {}),
  restart: (node: string) => post("/api/chaos/restart", { node }),
  isolate: (node: string) => post("/api/chaos/isolate", { node }),
  partition: (group: string[]) => post("/api/chaos/partition", { group }),
  heal: () => post("/api/chaos/heal", {}),
  recover: () => post("/api/chaos/recover", {}),
};

/**
 * subscribe opens the gateway's event stream and calls back on every
 * snapshot and batch of events. EventSource reconnects on its own, so a
 * gateway restart or a dropped connection heals without a page reload; the
 * status callback is how the header can say so while it's down.
 */
export function subscribe(handlers: {
  onSnapshot: (s: Snapshot) => void;
  onEvents: (e: ClusterEvent[]) => void;
  onStatus: (connected: boolean) => void;
}): () => void {
  const source = new EventSource("/api/stream");

  // A dropped stream is reported only if it stays dropped. EventSource
  // reconnects by itself within a few seconds, and announcing every blip
  // would put a warning on the page more often than anything is actually
  // wrong — which is how a status indicator stops being read.
  let offlineTimer: number | undefined;

  const online = () => {
    clearTimeout(offlineTimer);
    offlineTimer = undefined;
    handlers.onStatus(true);
  };

  const maybeOffline = () => {
    if (offlineTimer !== undefined) return;
    offlineTimer = window.setTimeout(() => handlers.onStatus(false), OFFLINE_GRACE_MS);
  };

  source.addEventListener("snapshot", (ev) => {
    online();
    handlers.onSnapshot(JSON.parse((ev as MessageEvent).data));
  });
  source.addEventListener("events", (ev) => {
    handlers.onEvents(JSON.parse((ev as MessageEvent).data));
  });
  source.addEventListener("error", maybeOffline);
  source.addEventListener("open", online);

  return () => {
    clearTimeout(offlineTimer);
    source.close();
  };
}

const OFFLINE_GRACE_MS = 4000;
