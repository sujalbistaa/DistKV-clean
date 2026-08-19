# Deploying the cluster and its console

This is what runs at **<http://34.240.41.1:8080>** — a `t3.micro` in `eu-west-1`
with the five-container stack on it, put there with `deploy/vm/setup.sh` and
nothing else. If that address ever moves, the link in the README moves with it.

Everything here assumes the goal is a public demo: a URL that shows a live
five-node cluster and lets a stranger break it. Running DistKV as real
infrastructure is a different exercise and the [limitations](../README.md#limitations)
are the place to start for that — no auth, no TLS, no membership changes.

---

## What has to run where

Five node processes and one gateway. The nodes are stateful, need a persistent
disk each, and must reach one another. The gateway is stateless and needs to
reach all five.

That rules out the serverless platforms — Vercel, Netlify, Cloudflare Workers —
for the *cluster*. Not because the code won't run there but because five
long-lived processes with their own disks and a consensus protocol between them
is precisely what those platforms exist not to do. The console is only a static
bundle and could live anywhere, but it is served by the gateway already, and
splitting them buys nothing but a CORS configuration.

So: one small machine, running `docker compose`. The whole thing fits
comfortably in 1 GB of RAM.

One container also works, with all six processes inside it, talking over
loopback instead of a bridge network. The consensus protocol has no opinion
about how many machines it is spread over — the nodes find each other by the
addresses in `-peers` either way — and that is what makes the free
platform-as-a-service options viable, since most of them will run one container
and expose one port. It is the same five processes with the same five write-ahead
logs; only the blast radius changes, and for a demo the blast radius is the
point of the buttons, not of the deployment.

---

## Hugging Face Spaces (needs a paid plan)

The shortest path to a public URL, and the only one here that needs no virtual
machine: a Docker Space runs the single-container arrangement described above.

**Check the price before you count on it.** Spaces' CPU Basic hardware is free,
but creating a Space that *runs compute* — Docker or Gradio, as opposed to a
Static one — requires a subscription; free accounts get Static only. That was
not true when this was first written, and it may not be true when you read it,
so look at <https://huggingface.co/pricing> rather than at this paragraph. If
you already have the plan, everything below works and takes about ten minutes.

Everything it needs is in [`deploy/huggingface/`](../deploy/huggingface/):
a `Dockerfile` that builds all six processes into one image, a `start.sh` that
brings them up, a Space `README.md` carrying the configuration Spaces read from
its front matter, and `publish.sh` to push it.

**1. Create the Space.** At <https://huggingface.co/new-space>, pick **Docker**
and the **blank** template, on CPU Basic hardware. Any name; `distkv` is fine.
Leave it public.

**2. Get a write token** from <https://huggingface.co/settings/tokens>.

**3. Push.**

```bash
export HF_TOKEN=hf_...
deploy/huggingface/publish.sh yourname/distkv
```

That assembles a staging tree — this working tree, with the Space's Dockerfile
and README in place of the repository's own — and force-pushes it. The first
build takes a few minutes because it compiles the console and three Go binaries;
watch the **Logs** tab, and "Running" means the cluster is up. The URL is
`https://yourname-distkv.hf.space`.

The Space seeds itself with a few keys on startup so the first thing a visitor
sees is a store with something in it.

### What to know before pointing anyone at it

**Storage is ephemeral.** A Space restart brings the cluster back empty. For a
demo that is closer to a feature than a limitation: it is also the only thing
that clears out whatever visitors have written. Persistent storage on Spaces is
a paid add-on and nothing here needs it.

**Free Spaces sleep.** After a long idle period a free Space is paused and the
first visitor after that waits for it to come back. If the link is going on a
CV, open it yourself now and then, or accept the cold start.

**The rate limiter needs the proxy header.** `start.sh` already passes
`-trust-proxy-headers`, which matters here: Spaces sit behind a proxy, so
without it every visitor in the world shares one budget.

**It is one container.** Five real processes with five real logs over real
sockets, but one blast radius: the platform restarting the container takes the
whole cluster with it, where five containers would lose one. Nothing a visitor
can do from the console causes that — the crash button destroys a node inside
its process precisely so it stays reachable enough to be restarted.

---

## Free: a virtual machine

The five-container arrangement, permanently on, at no cost. Three providers
have a free tier this fits inside, and they differ in one way that matters more
than the specifications:

| | | |
|---|---|---|
| **Oracle Cloud** | 4 ARM cores, 24 GB | Free forever. Capacity often unavailable. |
| **Google Cloud** | 1 shared vCPU, 1 GB | Free forever, in three US regions only. |
| **AWS EC2** | 2 burstable vCPU, 1 GB | **Free for 12 months, then about $7.50/month.** |

AWS's EC2 allowance is the one with an expiry date on it. Everything else about
it is the smoothest of the three — capacity is never a problem and the console
is the least fiddly — so it is a fine choice as long as the calendar is not a
surprise later. Set a billing alarm and put a note in your calendar for month
eleven.

All three want a credit card at signup, which is the price of admission and the
reason this is not simply the recommended option for everybody.

Whichever you pick, the deployment is the same three commands, and there is a
script that does all of it:

```bash
git clone https://github.com/sujalbistaa/DistKV.git
cd DistKV
deploy/vm/setup.sh                      # console on http://<your-ip>:8080
deploy/vm/setup.sh distkv.example.org   # or with HTTPS, if you have a name
```

It installs Docker if the machine hasn't got it, opens the host firewall if
something is blocking the port, and starts the stack. Read it first — it is
about eighty lines and it uses `sudo`.

### Oracle Cloud (Always Free)

The more generous by a wide margin: up to 4 Ampere ARM cores and 24 GB of RAM,
which is roughly twenty times what this needs.

1. Create an **Always Free** account, then a compute instance: shape
   **VM.Standard.A1.Flex**, 1 OCPU and 6 GB is plenty, image **Canonical
   Ubuntu 22.04**. Save the SSH key it offers you.
2. Open the port in the **VCN security list**: Networking → Virtual Cloud
   Networks → your VCN → Security Lists → Default → Add Ingress Rule, source
   `0.0.0.0/0`, destination port `8080` (or `80,443` if you are using a
   domain).
3. `ssh ubuntu@<public-ip>`, then the three commands above.

Two things to expect. Ampere capacity is frequently exhausted in popular
regions — "Out of host capacity" at instance creation is normal, and the usual
answer is to retry, or pick a less busy region at signup, or fall back to the
AMD micro shape (1 GB, still free, still enough). And Oracle's images ship with
a host firewall that rejects everything but SSH *in addition to* the cloud
security list, so a stack that comes up perfectly and answers nothing is almost
always that; `setup.sh` handles it.

### AWS EC2 (free for 12 months)

1. **Launch an instance.** EC2 → Launch instance.
   - **AMI: Ubuntu Server 22.04 or 24.04 LTS.** Not Amazon Linux — Docker's
     install script does not officially support it, and you would be
     hand-installing the compose plugin for no reason.
   - **Type: `t3.micro`** (or `t2.micro` in regions without t3). Both are
     free-tier eligible; `t3.micro` gets two burstable vCPUs instead of one.
   - **Key pair:** create one and keep the `.pem`.
   - **Storage:** the default 8 GB is enough; the free allowance is 30 GB.
2. **Security group.** This is the whole firewall on AWS — Ubuntu's AMIs do not
   block anything themselves, so there is nothing to open on the host.
   - SSH, TCP 22, **your IP only**.
   - Custom TCP, port **8080**, source `0.0.0.0/0` — or **80 and 443** instead
     if you are going to use a domain.
3. **Deploy.**
   ```bash
   ssh -i your-key.pem ubuntu@<public-ip>
   git clone https://github.com/sujalbistaa/DistKV.git
   cd DistKV
   deploy/vm/setup.sh
   ```

Three things specific to AWS:

**The public IP changes if you ever stop the instance.** Allocate an Elastic IP
and associate it — free while it is attached to a running instance, charged
only when it is left lying around. Skip this and the link in your CV eventually
points at somebody else's machine.

**Watch the free tier's clock, not just its size.** EC2's 12 months is per
account, not per instance. And accounts opened since mid-2025 are on a
different plan again — a fixed credit balance that expires after six months
rather than a 12-month allowance — so check which one you are on at
<https://console.aws.amazon.com/billing/home#/freetier> before assuming.

**Egress is 100 GB/month free**, which the console will not trouble unless the
link goes properly viral: an idle open tab costs about 2 GB a month, since the
gateway only sends a frame when the cluster's state actually changes.

`t3.micro` has 1 GB of RAM and burstable CPU, so the note below about election
timeouts on a shared core applies here too.

### Google Cloud (Always Free)

Smaller but more reliably available: one `e2-micro` per month in `us-west1`,
`us-central1`, or `us-east1`, with 1 GB of RAM and a shared vCPU.

1. Create the VM: machine type **e2-micro**, one of those three regions,
   boot disk **Ubuntu 22.04**, and tick **Allow HTTP traffic** (or add a
   firewall rule for `tcp:8080`).
2. SSH in from the console, then the three commands above.

1 GB is enough — six Go processes idle at something like 250 MB between them —
but a shared or burstable vCPU is worth knowing about, here and on AWS's
`t3.micro`. Raft's default election timeout is 150–300 ms, and on a throttled
core a scheduling delay looks exactly like a leader that has stopped sending
heartbeats, producing elections nobody asked for. If the console shows the
cluster changing leaders on its own while nothing is touching it, that is what
it is, and the fix is to give it more room — add these to each node's `command`
in `docker-compose.yml`:

```yaml
      - -election-timeout-min=400ms
      - -election-timeout-max=800ms
      - -heartbeat-interval=150ms
```

### A hostname, for free

Let's Encrypt will not issue a certificate for a bare IP address, so HTTPS
needs a name. A dynamic-DNS provider gives you one at no cost — DuckDNS is the
usual choice: register `something.duckdns.org`, point it at the VM's public
address, and pass it to `setup.sh`. That runs Caddy in front of the console
using [`deploy/vm/docker-compose.tls.yml`](../deploy/vm/docker-compose.tls.yml),
which obtains and renews the certificate on its own.

Without a name, `http://<ip>:8080` works perfectly well. It just reads like an
IP address in whatever you paste it into.

---

## Before it faces the internet

The defaults are tuned for a demo, not for a machine nobody is watching. Three
things to check — the fourth used to be publishing the node ports, and that is
now safe by default.

**The node ports are on loopback, and should stay there.** `docker-compose.yml`
publishes each node's gRPC port as `127.0.0.1:707x:7070`, so `distkv-cli` works
from the machine itself and from nowhere else. Do not widen those to `0.0.0.0`
on a public host: the KV API has no authentication of any kind, and anybody who
found it would have unrestricted write access to the store, around every limit
the console applies. The console's port is the one meant to be public.

**Turn on the proxy header trust — but only behind a proxy.** The rate limiter
keys on the client address. Behind nginx or Caddy every request appears to come
from the proxy, so one visitor's budget is shared by all of them; add
`-trust-proxy-headers` to the console's command so it reads `X-Forwarded-For`
instead. Do not set it if the gateway is exposed directly: a client can forge
that header and mint itself a fresh budget per request.

**Keep the auto-heal short.** `-auto-heal=5m` puts the cluster back together
after it has been left broken and idle for five minutes. Without it the first
visitor to partition the cluster and close the tab leaves it broken for everyone
after them. Shorter is friendlier for a demo that gets real traffic.

**Decide whether strangers may break it at all.** `-allow-chaos=false` leaves a
live, read-only view: the node table, the replication state, and the event log,
with the fault buttons gone. Worth having as an option even if you don't use it.

The other limits — `-max-keys`, `-max-key-len`, `-max-value-len`,
`-writes-per-minute`, `-faults-per-minute` — are on by default and are what
keeps a store anyone can write to from becoming a store anyone can fill. Their
defaults are in `gateway.DefaultOptions`.

---

## TLS

`deploy/vm/setup.sh <domain>` does this already, by layering
[`deploy/vm/docker-compose.tls.yml`](../deploy/vm/docker-compose.tls.yml) over
the main file to add Caddy and switch the console to `-trust-proxy-headers`.
By hand it is:

```bash
DISTKV_DOMAIN=distkv.example.org docker compose \
    -f docker-compose.yml \
    -f deploy/vm/docker-compose.tls.yml \
    up --build -d
```

Caddy obtains and renews the certificate on its own; there is nothing to
configure beyond the name.

One caveat applies to any proxy put in front of this, Caddy included: the
console holds a server-sent-events stream open indefinitely, so response
buffering has to be off and the read timeout has to be generous, or the live
view freezes every time the proxy gives up on the connection. The supplied
[`Caddyfile`](../deploy/vm/Caddyfile) sets `flush_interval -1` for exactly that
reason. For nginx it is `proxy_buffering off;` and a large `proxy_read_timeout`
on the `/api/stream` location; the gateway also sends `X-Accel-Buffering: no`,
which nginx respects on its own.

---

## Fly.io

Workable, with one wrinkle worth knowing before you start: the five nodes need
stable addresses for each other, which on Fly means five separate apps (or one
app with five machines addressed through internal DNS) rather than five
instances of one scaling app. The peer list is fixed at startup and the nodes
identify each other by the names in it.

If that sounds like more moving parts than a €4 virtual machine, it is. Fly is
the better answer when you want the demo to survive the machine dying; a single
host is the better answer for everything else.

---

## Keeping it up

```bash
docker compose ps            # what is running
docker compose logs -f console
docker compose restart node3 # exercises the real crash-recovery path
docker compose down          # keeps the volumes
docker compose down -v       # discards them: the cluster comes back empty
```

The nodes restart with `restart: unless-stopped`, so a reboot brings everything
back, each node recovering from its own write-ahead log. Data lives in named
Docker volumes; `docker compose down` on its own does not touch them.

If the demo ends up somewhere it can be found, watch the disk. Every write is a
log entry and log entries are only reclaimed by snapshotting, which is
threshold-driven (`-snapshot-threshold`, 10,000 entries by default). The key cap
bounds the size of the *state*, not the length of the log that produced it.
