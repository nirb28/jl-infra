# jl-infra — Implementation Plan

A chat-driven replacement for Posit Workbench: an **MCP server + control plane** that starts,
lists, and kills interactive IDE sessions (JupyterLab first, VS Code later) across many machines,
with **pluggable resource managers** (Slurm first, then Kubernetes and a plain load-balanced
server pool) and an **nginx reverse proxy** so users reach their sessions in a browser.

This document is the spec for implementation. Work through the phases in order; each phase has
acceptance criteria. Do not skip the plugin abstractions in Phase 1 — they are the point of the
project.

---

## 1. Goals and non-goals

**Goals**
- Start / list / inspect / kill interactive sessions via natural-language chat (MCP tools) and via REST.
- Sessions run on compute managed by a pluggable backend: `slurm` (v1), `ssh_pool` (v1.5), `kubernetes` (v2).
- Pluggable "launchers" for what runs inside the session: `jupyterlab` (v1), `code-server` (v2), generic template launcher.
- Browser access to every session through a single entry host via nginx reverse proxy (WebSocket-capable).
- Single YAML config file describing backends, launchers, proxy, and defaults.
- State survives control-plane restarts (SQLite); a reconciler re-syncs with backend truth.

**Non-goals (for now)**
- Full multi-tenant auth (LDAP/OAuth). Design the seams, implement trusted single-user first.
- Billing/quotas, session sharing, collaborative editing.
- Replacing Slurm scheduling logic — we submit jobs, we don't schedule.

---

## 2. High-level architecture

```
 Claude Desktop / Claude Code / custom chat UI
                 │  (MCP: stdio or streamable-HTTP)
                 ▼
        ┌───────────────────┐        ┌──────────────┐
        │    MCP Server      │───────▶│  Control     │
        │  (thin tool layer) │        │  Plane API   │  FastAPI, one process
        └───────────────────┘        │  + Session   │  (MCP mounted in-process)
                 ▲                    │  Manager     │
    REST clients ┴───────────────────▶│  + Reconciler│
                                      └──┬───────┬───┘
                                         │       │
                          ComputeBackend │       │ ProxyManager
                        ┌────────────────┤       └───────────────┐
                        ▼                ▼                       ▼
                 ┌────────────┐   ┌────────────┐          ┌────────────┐
                 │ SlurmBackend│  │ SshPool    │          │ NginxProxy │
                 │ sbatch/     │  │ Backend    │          │ conf.d/*.conf
                 │ squeue/     │  │ (asyncssh, │          │ + reload   │
                 │ scancel     │  │  load-based│          └────────────┘
                 └─────┬──────┘   │  placement)│
                       ▼          └─────┬──────┘
                 compute nodes ◀────────┘
                 (jupyter lab / code-server processes, one port+token per session)

User browser ──▶ https://hub.example.com/session/<id>/ ──nginx──▶ node:port
```

Key decisions:
- **One Python process** hosts both the FastAPI REST API and the MCP server (the official
  `mcp` SDK's streamable-HTTP app mounts into FastAPI/Starlette). stdio transport also supported
  for local Claude Desktop/Code use — same tool implementations either way.
- **MCP tools are thin**: they call the same `SessionManager` service the REST API calls.
  No business logic in the tool layer.
- **Backends own placement**; the control plane owns lifecycle, state, and proxying.

---

## 3. Tech stack

| Concern | Choice | Notes |
|---|---|---|
| Language | Python ≥ 3.11 | async throughout |
| API | FastAPI + uvicorn | |
| MCP | official `mcp` Python SDK (FastMCP API) | stdio + streamable-HTTP transports |
| DB | SQLite via SQLAlchemy 2.x (async) | schema kept portable to Postgres |
| SSH | `asyncssh` | for ssh_pool backend and remote slurm submission |
| K8s | `kubernetes` official client | Phase 5 |
| Templates | Jinja2 | sbatch scripts, nginx conf, launcher commands |
| Config | pydantic-settings + YAML | validated at startup |
| Packaging | `uv` + `pyproject.toml` | console script `jl-infra` |
| Tests | pytest + pytest-asyncio; mock backend; slurm-docker-cluster for integration | |

---

## 4. Repository layout

```
jl-infra/
├── pyproject.toml
├── config/
│   ├── config.example.yaml
│   └── templates/
│       ├── slurm/jupyterlab.sbatch.j2
│       ├── slurm/code-server.sbatch.j2
│       └── nginx/session.conf.j2
├── src/jl_infra/
│   ├── main.py                  # entrypoint: CLI (typer) → serve / mcp-stdio / status
│   ├── config.py                # pydantic models for the YAML config
│   ├── models.py                # Session, Node, enums (SQLAlchemy + pydantic DTOs)
│   ├── db.py
│   ├── manager.py               # SessionManager: the ONLY place lifecycle logic lives
│   ├── reconciler.py            # background loop syncing DB ↔ backend truth
│   ├── registry.py              # plugin registries (entry-point + config driven)
│   ├── api/
│   │   ├── rest.py              # /api/v1 routes
│   │   └── mcp_server.py        # MCP tool definitions (thin wrappers)
│   ├── backends/
│   │   ├── base.py              # ComputeBackend ABC
│   │   ├── slurm.py
│   │   ├── ssh_pool.py
│   │   ├── kubernetes.py
│   │   └── mock.py              # for tests
│   ├── launchers/
│   │   ├── base.py              # Launcher ABC
│   │   ├── jupyterlab.py
│   │   ├── code_server.py
│   │   └── generic.py           # fully template-driven
│   ├── proxy/
│   │   ├── base.py              # ProxyManager ABC
│   │   ├── nginx.py
│   │   └── noop.py
│   └── balancing/
│       ├── base.py              # NodeSelector ABC
│       └── least_loaded.py
├── tests/
│   ├── unit/ ...
│   └── integration/ ...
└── docs/
    ├── IMPLEMENTATION_PLAN.md   # this file
    └── ADRs as needed
```

---

## 5. Data model

```python
class SessionState(str, Enum):
    PENDING   = "pending"     # accepted, submitting to backend
    STARTING  = "starting"    # backend placed it, waiting for readiness probe
    RUNNING   = "running"     # ready, proxy route registered
    STOPPING  = "stopping"
    STOPPED   = "stopped"     # terminal
    FAILED    = "failed"      # terminal, keep error message

class Session:                # SQLAlchemy table `sessions`
    id: str                   # short slug e.g. "jl-a1b2c3" — used in URL path
    user: str
    launcher: str              # "jupyterlab" | "code-server" | ...
    backend: str               # "slurm" | "ssh_pool" | "kubernetes"
    state: SessionState
    # placement (filled by backend once known)
    node: str | None           # hostname/IP the process runs on
    port: int | None
    backend_ref: str | None    # slurm job id / k8s pod name / "host:pid"
    # access
    token: str                 # per-session secret injected into launcher
    url_path: str              # "/session/<id>/"
    # request
    resources: dict            # {"cpus": 4, "mem_gb": 16, "gpus": 1, "partition": "gpu", "time_limit": "8:00:00"}
    env: dict
    created_at / started_at / finished_at: datetime
    error: str | None
```

Rules:
- `id` is globally unique, URL-safe, ≤ 16 chars. It is the proxy path segment and the backend job name.
- `token` is generated with `secrets.token_urlsafe(32)`; never logged; stored in DB (acceptable v1,
  note as hardening item).
- All state transitions happen in `SessionManager` only, each written to DB before/after backend calls
  so a crash mid-operation is recoverable by the reconciler.

---

## 6. Plugin interfaces (write these first)

### 6.1 ComputeBackend (`backends/base.py`)

```python
class LaunchSpec(BaseModel):
    session_id: str
    command: list[str]         # from Launcher, with {port}/{token} already resolved OR
    command_template: str      # backend may prefer rendering itself (slurm sbatch)
    resources: Resources
    env: dict[str, str]
    port_hint: int | None      # ssh_pool picks free port itself; slurm picks random in range

class Placement(BaseModel):
    node: str
    port: int
    backend_ref: str

class BackendStatus(str, Enum):
    QUEUED = "queued"; RUNNING = "running"; FINISHED = "finished"
    FAILED = "failed"; UNKNOWN = "unknown"

class ComputeBackend(ABC):
    name: str
    async def submit(self, spec: LaunchSpec) -> str: ...          # returns backend_ref
    async def status(self, backend_ref: str) -> BackendStatus: ...
    async def placement(self, backend_ref: str) -> Placement | None: ...  # None until known
    async def cancel(self, backend_ref: str) -> None: ...
    async def logs(self, backend_ref: str, tail: int = 100) -> str: ...
    async def list_refs(self) -> list[str]: ...                   # everything this backend runs for us
    async def cluster_info(self) -> dict: ...                     # partitions / node load / capacity
```

### 6.2 Launcher (`launchers/base.py`)

```python
class Launcher(ABC):
    name: str
    async def build_command(self, session: Session, port: int, token: str,
                            base_url: str) -> list[str]: ...
    # base_url is the external prefix (e.g. /session/jl-a1b2c3) — jupyter needs
    # --ServerApp.base_url; code-server is prefix-agnostic behind a stripping proxy
    async def readiness_url(self, node: str, port: int, base_url: str) -> str: ...
    async def is_ready(self, node: str, port: int, base_url: str, token: str) -> bool: ...
    def proxy_flavor(self) -> ProxyFlavor: ...   # PRESERVE_PREFIX (jupyter) vs STRIP_PREFIX (code-server)
```

`jupyterlab` launcher v1 command (rendered into sbatch or ssh exec):

```
jupyter lab --no-browser --ip=0.0.0.0 --port={port} \
  --ServerApp.base_url={base_url} \
  --ServerApp.allow_origin='*' \
  --IdentityProvider.token={token} \
  --ServerApp.root_dir={workdir}
```

Readiness: `GET http://{node}:{port}{base_url}/api` returns 200/302 → ready.

### 6.3 ProxyManager (`proxy/base.py`)

```python
class ProxyManager(ABC):
    async def add_route(self, session: Session) -> str: ...   # returns public URL
    async def remove_route(self, session: Session) -> None: ...
    async def sync(self, sessions: list[Session]) -> None: ... # full rewrite (reconciler)
```

**NginxProxyManager**: writes one file per session into `conf_d_dir` from
`templates/nginx/session.conf.j2`, then runs `reload_cmd` (default
`nginx -s reload`, configurable — e.g. `sudo systemctl reload nginx` or
`docker kill -s HUP nginx`). Template essentials:

```nginx
location /session/{{ id }}/ {
    proxy_pass http://{{ node }}:{{ port }}{% if flavor == "strip" %}/{% endif %};
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;      # jupyter kernels = websockets
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_read_timeout 86400;
    client_max_body_size 0;                      # notebook uploads
}
```

A static top-level `jl-infra.conf` (installed once, documented in README) includes
`conf_d_dir/*.conf` inside a `server {}` block and proxies `/api/` + `/mcp/` to the control plane.

### 6.4 NodeSelector (`balancing/base.py`) — used by ssh_pool only

```python
class NodeSelector(ABC):
    async def pick(self, nodes: list[NodeInfo], resources: Resources) -> NodeInfo: ...

class NodeInfo(BaseModel):
    host: str
    cpus_total: int; cpus_used: int
    mem_gb_total: float; mem_gb_free: float
    gpus_total: int; gpus_free: int
    load_1m: float
    sessions: int              # ours
```

`least_loaded` v1: filter nodes that fit the request, score =
`0.5 * load_1m/cpus_total + 0.3 * (1 - mem_free/mem_total) + 0.2 * sessions/max_sessions`, pick min.
Metrics gathered over SSH: `nproc`, `/proc/loadavg`, `free -g`, `nvidia-smi --query-gpu=...` (if configured).

### 6.5 Registration

`registry.py` maps config names → classes. Built-ins registered directly; third-party plugins
discoverable via entry points group `jl_infra.backends` / `jl_infra.launchers` (document, wire, done —
no dynamic loading magic beyond `importlib.metadata.entry_points`).

---

## 7. MCP server — tool spec

Transport: streamable-HTTP mounted at `/mcp` (primary, works from anywhere) and a
`jl-infra mcp-stdio` command for local hosts. Tools return **structured JSON content** plus a short
human-readable summary string — the model reads both.

| Tool | Params | Behavior |
|---|---|---|
| `start_session` | `launcher="jupyterlab"`, `backend=None` (default from config), `cpus`, `mem_gb`, `gpus`, `partition`, `time_limit`, `workdir`, `name` | Creates session, returns `{id, state, url}` immediately (state=pending/starting). Does **not** block until running. |
| `list_sessions` | `user=None`, `state=None`, `all_users=False` | Table of sessions with id, launcher, backend, state, node, age, url. |
| `get_session` | `session_id` | Full detail incl. resources, error, url. |
| `kill_session` | `session_id` | Idempotent; also accepts `all_mine=True` to kill every session of the caller. |
| `get_session_logs` | `session_id`, `tail=100` | Backend logs (slurm .out file / ssh captured output / pod logs). |
| `wait_until_ready` | `session_id`, `timeout_s=180` | Polls until RUNNING or FAILED; use after start_session. |
| `get_cluster_status` | `backend=None` | Partitions, node counts, load — from `cluster_info()`. |
| `list_capabilities` | — | Configured backends, launchers, defaults, resource limits. Lets the chatbot answer "what can I ask for?" |

Guidelines for implementation:
- Every tool docstring written for the LLM: what it does, when to use it, what the params mean,
  units (mem in **GB**, time_limit as `HH:MM:SS`).
- Errors returned as structured `{error: str, hint: str}` — never raise raw tracebacks into MCP.
- `user` comes from the auth context (v1: config `default_user` or `X-User` header), not from the model.

---

## 8. REST API (mirrors the tools)

```
POST   /api/v1/sessions              # body = same fields as start_session
GET    /api/v1/sessions?user=&state=
GET    /api/v1/sessions/{id}
DELETE /api/v1/sessions/{id}
GET    /api/v1/sessions/{id}/logs?tail=
GET    /api/v1/cluster?backend=
GET    /api/v1/capabilities
GET    /healthz
```

REST and MCP both delegate to `SessionManager` — one implementation, two front doors.

---

## 9. Configuration schema (`config.example.yaml`)

```yaml
server:
  host: 0.0.0.0
  port: 8000
  external_url: https://hub.example.com     # what users see; proxy fronts this
  db_path: /var/lib/jl-infra/state.db
  default_user: nirb28

defaults:
  backend: slurm
  launcher: jupyterlab
  resources: { cpus: 2, mem_gb: 8, gpus: 0, time_limit: "08:00:00" }
  session_ttl_hours: 24          # reconciler kills sessions older than this (0 = off)

backends:
  slurm:
    type: slurm
    mode: cli                    # cli | ssh_cli | rest
    ssh:                         # only for ssh_cli mode (control plane not on login node)
      host: login01.cluster
      user: nirb28
      keyfile: ~/.ssh/id_ed25519
    sbatch_template: slurm/jupyterlab.sbatch.j2   # per-launcher override supported
    port_range: [20000, 30000]
    partitions_allowed: [interactive, gpu]
    log_dir: /home/{user}/.jl-infra/logs

  onprem_pool:
    type: ssh_pool
    selector: least_loaded
    connect: { user: nirb28, keyfile: ~/.ssh/id_ed25519 }
    port_range: [20000, 30000]
    nodes:
      - { host: gpu01.corp, gpus: 4, max_sessions: 8 }
      - { host: gpu02.corp, gpus: 4, max_sessions: 8 }
      - { host: cpu01.corp, max_sessions: 16 }

  # k8s:
  #   type: kubernetes
  #   namespace: jl-infra
  #   image: quay.io/jupyter/scipy-notebook:latest

launchers:
  jupyterlab:
    type: jupyterlab
    executable: jupyter          # or a conda env / module load in the template
    workdir: /home/{user}
  code-server:
    type: code_server
    executable: code-server

proxy:
  type: nginx
  conf_d_dir: /etc/nginx/jl-infra.d
  reload_cmd: ["sudo", "nginx", "-s", "reload"]
  public_base: https://hub.example.com
```

Validation at startup with clear errors ("backend 'slurm' references template X which does not exist").

---

## 10. Session lifecycle (SessionManager)

**start**
1. Validate request against config limits → create row (PENDING), generate id + token.
2. Launcher builds command/template context; backend `submit()` → save `backend_ref` (STARTING).
3. Return immediately. Background task (or reconciler tick) polls `placement()`; when node+port known,
   run readiness probe (with per-launcher timeout, default 180 s).
4. Ready → `proxy.add_route()` → RUNNING, store final URL `{public_base}/session/{id}/`.
5. Probe timeout or backend FAILED → mark FAILED, fetch last log lines into `error`, remove any route.

**kill**
1. STOPPING → `proxy.remove_route()` (best-effort) → `backend.cancel()` → STOPPED.

**Reconciler** (asyncio task, every 30 s):
- For each non-terminal session: backend `status()`; fix drift (job died → FAILED/STOPPED; job running
  but we think STARTING → re-probe).
- Enforce `session_ttl_hours`.
- `proxy.sync()` full rewrite if drift detected or on startup (control-plane restart must restore all routes).
- Orphan detection: `backend.list_refs()` entries with no DB row → log loudly, optionally cancel (config flag).

---

## 11. Slurm backend specifics (Phase 2 — the critical one)

- **Submission**: render `jupyterlab.sbatch.j2`, roughly:

  ```bash
  #SBATCH --job-name=jl-infra-{{ session_id }}
  #SBATCH --cpus-per-task={{ cpus }} --mem={{ mem_gb }}G --time={{ time_limit }}
  {% if gpus %}#SBATCH --gres=gpu:{{ gpus }}{% endif %}
  #SBATCH --output={{ log_dir }}/{{ session_id }}.out
  PORT=$(shuf -i {{ port_lo }}-{{ port_hi }} -n 1)   # retry loop: check with `ss -ltn`
  echo "JL_INFRA_PLACEMENT node=$(hostname -f) port=$PORT"
  exec {{ command }}          # with $PORT substituted
  ```

- **Placement discovery**: `squeue -j <id> -h -o "%N %T"` gives node + state; the **port** is read
  from the `JL_INFRA_PLACEMENT` line in the job's output file (simplest reliable channel; avoids
  needing a callback from compute node to control plane). Poll both.
- **Status mapping**: PD→QUEUED, R→RUNNING, CG/CD→FINISHED, F/TO/OOM/NF→FAILED, unknown job (aged
  out of squeue) → check `sacct` if available, else UNKNOWN.
- **Modes**: `cli` (control plane runs where slurm commands work), `ssh_cli` (same commands over
  asyncssh to a login node — same code path, the command runner is injected), `rest`
  (slurmrestd, stub for later; the command-runner abstraction lets this slot in).
- **Cancel**: `scancel <jobid>`.

---

## 12. Security & hardening checklist

v1 (must):
- Per-session random token, enforced by Jupyter/code-server themselves — a leaked URL without the
  token is useless.
- Control plane binds localhost or is only reachable via nginx; nginx does TLS.
- MCP HTTP endpoint requires a static bearer token from config (`Authorization: Bearer ...`).
- Never interpolate user input into shell strings — always argv lists / Jinja with a `shlex.quote`
  filter applied to every user-influenced variable.

Documented for later:
- Real user auth (OIDC) + per-user identity on MCP; nginx `auth_request` on session routes
  (currently token-only); secrets out of DB into keyring/vault; rate limits.

---

## 13. Implementation phases

**Phase 1 — Skeleton + mock backend (foundation)**
Scaffold repo, config loading, DB, models, SessionManager, registries, REST API, MCP server with all
tools, `mock` backend (asyncio subprocess on localhost running `python -m http.server` as a fake
IDE), `noop` proxy.
✔ Accept: `pytest` green; via MCP inspector or Claude Code: start → list → logs → kill a mock
session; state survives a process restart.

**Phase 2 — Slurm backend + JupyterLab launcher**
As §11 + §6.2. Integration test against `giovtorres/slurm-docker-cluster` (docker-compose in
`tests/integration/`).
✔ Accept: from chat, "start me a jupyter session with 4 cpus on partition interactive" yields a
RUNNING session with a reachable `http://node:port/session/<id>/api`; kill works; job death is
detected by the reconciler.

**Phase 3 — Nginx proxy**
NginxProxyManager + session template + top-level conf + docs. Compose file adding an nginx
container for local dev.
✔ Accept: browser reaches JupyterLab at `https://hub/session/<id>/`, kernels work (websockets),
route removed on kill, routes restored after control-plane restart.

**Phase 4 — ssh_pool backend + least_loaded selector**
asyncssh runner, metric collection, placement, process supervision via
`nohup ... & echo $! > pidfile` + liveness `kill -0`, logs captured to a per-session file on the node.
✔ Accept: two-node docker-compose (sshd containers) — sessions spread by load; node down →
sessions marked FAILED by reconciler.

**Phase 5 — code-server launcher + kubernetes backend**
code-server with STRIP_PREFIX proxying; k8s backend = pod per session (readiness via pod IP:port,
cancel = delete pod, logs = pod logs).
✔ Accept: VS Code in browser via proxy; the same MCP tools drive k8s sessions with `backend="k8s"`.

**Phase 6 — Polish**
TTL enforcement, `list_capabilities`, README with Claude Desktop / Claude Code MCP connection
snippets, optional minimal web chat UI (defer unless asked).

---

## 14. Testing strategy

- Unit: SessionManager state machine against the `mock` backend (happy path, submit failure, probe
  timeout, crash-recovery: kill the manager between submit and placement, restart, reconciler
  completes the start).
- Contract tests: one shared test class run against every backend implementation (mock always;
  slurm/k8s behind pytest markers requiring their docker-compose stacks).
- Proxy: golden-file tests for rendered nginx configs; integration via compose.
- MCP: `mcp` SDK client in-process — call each tool, assert the structured output schema.

---

## 15. Conventions for the implementer

- Type hints everywhere, `ruff check` + `ruff format`, no bare `except`.
- Every backend call that shells out logs the exact argv at DEBUG level.
- Timeouts on all subprocess/SSH/HTTP calls — nothing may hang the reconciler.
- Small commits per phase step; each phase ends with its acceptance criteria demonstrably passing.
