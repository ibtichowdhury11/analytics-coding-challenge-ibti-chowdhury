#!/usr/bin/env python3
"""
Lightdash auto-setup script.
Creates default user/org/project and seeds a single neutral starter chart.
"""
import http.cookiejar
import json
import os
import socket
import struct
import sys
import time
import urllib.error
import urllib.request

LIGHTDASH_URL = os.environ.get("LIGHTDASH_URL", "http://lightdash:8080")
ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "admin@lightdash.com")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin123!")
ADMIN_FIRST = os.environ.get("ADMIN_FIRST", "Admin")
ADMIN_LAST = os.environ.get("ADMIN_LAST", "User")
ORG_NAME = os.environ.get("ORG_NAME", "Venatus")
PROJECT_NAME = os.environ.get("PROJECT_NAME", "Ad Analytics")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")

# Lightdash internal Postgres (trust auth — for onboarding dismissal only)
PGHOST = os.environ.get("PGHOST", "lightdash-db")
PGPORT = os.environ.get("PGPORT", "5432")
PGUSER = os.environ.get("PGUSER", "lightdash")
PGDATABASE = os.environ.get("PGDATABASE", "lightdash")

# Dashboard template directory (mounted from ./lightdash)
DASHBOARD_DIR = os.environ.get("DASHBOARD_DIR", "/lightdash")
CHARTS_DIR = os.path.join(DASHBOARD_DIR, "charts")

cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))


def log(msg):
    print(f"[lightdash-setup] {msg}", flush=True)


def api(method, path, data=None):
    url = f"{LIGHTDASH_URL}{path}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Content-Type", "application/json")
    try:
        resp = opener.open(req, timeout=30)
        return json.loads(resp.read().decode())
    except urllib.error.HTTPError as err:
        body_text = err.read().decode()
        try:
            return json.loads(body_text)
        except json.JSONDecodeError:
            return {"status": "error", "error": {"message": body_text, "statusCode": err.code}}


def wait_for_lightdash(timeout=120):
    log(f"Waiting for Lightdash at {LIGHTDASH_URL} ...")
    for _ in range(timeout):
        try:
            req = urllib.request.Request(f"{LIGHTDASH_URL}/api/v1/livez")
            resp = urllib.request.urlopen(req, timeout=3)
            if resp.status == 200:
                log("Lightdash is ready.")
                return
        except Exception:
            pass
        time.sleep(1)
    log("ERROR: Lightdash did not become ready in time")
    sys.exit(1)


def check_existing_setup():
    resp = api("POST", "/api/v1/login", {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    if resp.get("status") == "ok":
        if (resp.get("results") or {}).get("isSetupComplete"):
            log("Setup already complete.")
            return True
        log("User exists but setup not complete — continuing...")
    return False


def get_project_uuid():
    resp = api("GET", "/api/v1/org/projects")
    if resp.get("status") != "ok":
        log(f"ERROR: Failed to list projects: {resp}")
        sys.exit(1)
    projects = resp.get("results", [])
    for project in projects:
        if project.get("name") == PROJECT_NAME:
            return project.get("projectUuid")
    if projects:
        return projects[0].get("projectUuid")
    log("ERROR: No project found in organization.")
    sys.exit(1)


def get_shared_space_uuid(project_uuid):
    resp = api("GET", f"/api/v1/projects/{project_uuid}/spaces")
    if resp.get("status") != "ok":
        log(f"ERROR: Failed to list spaces: {resp}")
        sys.exit(1)
    spaces = resp.get("results", [])
    for space in spaces:
        if space.get("name") == "Shared":
            return space.get("uuid")
    if spaces:
        return spaces[0].get("uuid")
    log("ERROR: No space found in project.")
    sys.exit(1)


def register_user():
    resp = api("POST", "/api/v1/user", {
        "firstName": ADMIN_FIRST,
        "lastName": ADMIN_LAST,
        "email": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD,
    })
    if resp.get("status") == "ok":
        log(f"User registered: {ADMIN_EMAIL}")
        return
    err = resp.get("error", {})
    if "AlreadyExistsError" in err.get("name", ""):
        login_resp = api("POST", "/api/v1/login", {"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
        if login_resp.get("status") != "ok":
            log(f"ERROR: Failed to login: {login_resp}")
            sys.exit(1)
        return
    log(f"ERROR: Failed to register user: {resp}")
    sys.exit(1)


def create_organization():
    user_resp = api("GET", "/api/v1/user")
    if (user_resp.get("results") or {}).get("organizationUuid"):
        return
    resp = api("PUT", "/api/v1/org", {"name": ORG_NAME})
    if resp.get("status") != "ok":
        log(f"ERROR: Failed to create organization: {resp}")
        sys.exit(1)


def create_project():
    resp = api("GET", "/api/v1/org/projects")
    if resp.get("results", []):
        return
    payload = {
        "name": PROJECT_NAME,
        "type": "DEFAULT",
        "dbtVersion": "v1.9",
        "dbtConnection": {"type": "none"},
        "warehouseConnection": {
            "type": "clickhouse",
            "host": CLICKHOUSE_HOST,
            "port": CLICKHOUSE_PORT,
            "user": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
            "schema": "raw",
            "secure": False,
        },
    }
    resp = api("POST", "/api/v1/org/projects", payload)
    if resp.get("status") != "ok":
        log(f"ERROR: Failed to create project: {resp}")
        sys.exit(1)


def pg_exec(sql):
    """Run a SQL statement against the Lightdash Postgres DB using the
    simple-query wire protocol (trust auth — no password exchange)."""

    def _read_n(sock, n):
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("pg socket closed")
            buf += chunk
        return buf

    def _read_msg(sock):
        hdr = _read_n(sock, 5)
        tag = chr(hdr[0])
        length = struct.unpack("!I", hdr[1:5])[0]
        body = _read_n(sock, length - 4) if length > 4 else b""
        return tag, body

    s = socket.create_connection((PGHOST, int(PGPORT)), timeout=10)
    try:
        # Startup message (protocol 3.0)
        params = f"user\x00{PGUSER}\x00database\x00{PGDATABASE}\x00\x00".encode()
        s.sendall(struct.pack("!II", len(params) + 8, 196608) + params)

        # Wait for ReadyForQuery (trust auth sends AuthOk + ReadyForQuery)
        while True:
            tag, body = _read_msg(s)
            if tag == "E":
                raise RuntimeError(f"PG error: {body.decode('utf-8', 'replace')}")
            if tag == "Z":
                break

        # Simple query
        q_payload = (sql + "\x00").encode()
        s.sendall(b"Q" + struct.pack("!I", len(q_payload) + 4) + q_payload)

        while True:
            tag, body = _read_msg(s)
            if tag == "E":
                log(f"PG query error: {body.decode('utf-8', 'replace')}")
            if tag == "Z":
                break
    finally:
        s.close()


def complete_setup():
    # Mark initial registration wizard as complete
    api("PATCH", "/api/v1/user/me/complete", {
        "isTrackingAnonymized": True,
        "isMarketingOptedIn": False,
    })
    api("PATCH", "/api/v1/user/me", {"isSetupComplete": True})
    log("Registration wizard marked as complete.")


# ---- Auto-discovery helpers ------------------------------------------------

def discover_chart_configs():
    """Find and load all *.json chart definitions in the charts directory."""
    configs = []
    if not os.path.isdir(CHARTS_DIR):
        log(f"Charts directory not found: {CHARTS_DIR}")
        return configs
    for fname in sorted(os.listdir(CHARTS_DIR)):
        if fname.endswith(".json"):
            path = os.path.join(CHARTS_DIR, fname)
            with open(path) as f:
                configs.append(json.load(f))
            log(f"Loaded chart config: {fname}")
    return configs


def discover_dashboard_configs():
    """Find and load all *.json dashboard definitions in the dashboard dir."""
    configs = []
    if not os.path.isdir(DASHBOARD_DIR):
        log(f"Dashboard directory not found: {DASHBOARD_DIR}")
        return configs
    for fname in sorted(os.listdir(DASHBOARD_DIR)):
        if fname.endswith(".json"):
            path = os.path.join(DASHBOARD_DIR, fname)
            with open(path) as f:
                configs.append(json.load(f))
            log(f"Loaded dashboard config: {fname}")
    return configs


# ---- Chart & dashboard upsert ----------------------------------------------

def ensure_chart(project_uuid, space_uuid, chart_def, existing_charts):
    """Create or update a SQL runner chart from a chart definition dict."""
    sql = chart_def["sql"]
    config = chart_def["config"]
    name = chart_def.get("name", "Untitled Chart")
    slug = chart_def.get("slug", "untitled-chart")
    desc = chart_def.get("description", "")
    limit = chart_def.get("limit", 100)

    for chart in existing_charts:
        if chart.get("slug") == slug:
            update = api("PATCH", f"/api/v1/projects/{project_uuid}/sqlRunner/saved/{chart.get('uuid')}", {
                "versionedData": {"sql": sql, "limit": limit, "config": config},
                "unversionedData": {
                    "spaceUuid": space_uuid,
                    "name": name,
                    "description": desc,
                },
            })
            if update.get("status") != "ok":
                log(f"ERROR: Failed to update chart '{slug}': {update}")
                sys.exit(1)
            log(f"Updated chart: {name} ({slug})")
            return slug

    created = api("POST", f"/api/v1/projects/{project_uuid}/sqlRunner/saved", {
        "spaceUuid": space_uuid,
        "name": name,
        "description": desc,
        "sql": sql,
        "limit": limit,
        "config": config,
        "slug": slug,
    })
    if created.get("status") != "ok":
        log(f"ERROR: Failed to create chart '{slug}': {created}")
        sys.exit(1)
    log(f"Created chart: {name} ({slug})")
    return (created.get("results") or {}).get("slug") or slug


def ensure_dashboard(project_uuid, space_uuid, dashboard_def, existing_dashboards):
    """Create or update a dashboard from a dashboard definition dict.
    Returns the dashboard UUID."""
    name = dashboard_def["name"]
    slug = dashboard_def["slug"]
    desc = dashboard_def.get("description", "")
    space_slug = dashboard_def.get("spaceSlug", "shared")
    tiles = dashboard_def.get("tiles", [])
    tabs = dashboard_def.get("tabs", [])
    version = dashboard_def.get("version", 1)

    # Ensure the dashboard exists (code API needs an existing dashboard)
    dashboard_uuid = None
    for dash in existing_dashboards:
        if dash.get("slug") == slug:
            dashboard_uuid = dash.get("uuid")
            break

    if not dashboard_uuid:
        resp = api("POST", f"/api/v1/projects/{project_uuid}/dashboards", {
            "name": name,
            "description": desc,
            "tabs": [],
            "tiles": [],
            "spaceUuid": space_uuid,
        })
        if resp.get("status") != "ok":
            log(f"ERROR: Failed to create dashboard '{slug}': {resp}")
            sys.exit(1)
        dashboard_uuid = (resp.get("results") or {}).get("uuid")

    # Upsert tiles via dashboard-as-code API
    resp = api("POST", f"/api/v1/projects/{project_uuid}/dashboards/{slug}/code", {
        "name": name,
        "version": version,
        "slug": slug,
        "spaceSlug": space_slug,
        "tabs": tabs,
        "tiles": tiles,
    })
    if resp.get("status") != "ok":
        log(f"ERROR: Failed to upsert dashboard '{slug}': {resp}")
        sys.exit(1)

    log(f"Synced dashboard: {name} ({slug})")

    # Pin to homepage if requested
    if dashboard_def.get("pinToHomepage", False):
        pin_to_homepage(dashboard_uuid)

    return dashboard_uuid


def pin_to_homepage(dashboard_uuid):
    """Pin the starter dashboard to the Lightdash project homepage."""
    if not dashboard_uuid:
        log("WARNING: No dashboard UUID to pin.")
        return
    # Toggle pin endpoint — idempotent as a toggle, so check first
    dash = api("GET", f"/api/v1/dashboards/{dashboard_uuid}")
    if dash.get("status") == "ok":
        pinned = (dash.get("results") or {}).get("pinnedListUuid")
        if pinned:
            log("Dashboard already pinned.")
            return
    resp = api("PATCH", f"/api/v1/dashboards/{dashboard_uuid}/pinning", {})
    if resp.get("status") != "ok":
        log(f"WARNING: Could not pin dashboard: {resp}")
    else:
        log("Starter dashboard pinned to homepage.")


def dismiss_onboarding(project_uuid):
    """Dismiss the 'Welcome — Run your first query' home screen by ensuring
    the onboarding row in Lightdash's Postgres DB has ``ranQuery_at`` set.
    Also runs a trivial SQL query via the API for good measure."""
    # Run a trivial query so Lightdash records activity
    try:
        api("POST", f"/api/v1/projects/{project_uuid}/sqlRunner/run", {
            "sql": "SELECT 1 AS ok",
            "limit": 1,
        })
    except Exception:
        pass

    # Directly ensure onboarding is marked complete in the DB
    try:
        pg_exec(
            "INSERT INTO onboarding (organization_id, \"ranQuery_at\", \"shownSuccess_at\") "
            "SELECT organization_id, NOW(), NOW() "
            "FROM organizations "
            "WHERE organization_id NOT IN (SELECT organization_id FROM onboarding) "
            "LIMIT 1"
        )
        pg_exec(
            "UPDATE onboarding "
            "SET \"ranQuery_at\" = NOW(), \"shownSuccess_at\" = NOW() "
            "WHERE \"ranQuery_at\" IS NULL"
        )
        log("Onboarding dismissed (ranQuery_at set in DB).")
    except Exception as exc:
        log(f"WARNING: Could not dismiss onboarding via DB: {exc}")


# (chart and dashboard ensure functions defined above)


def main():
    wait_for_lightdash()
    if not check_existing_setup():
        register_user()
        create_organization()
        create_project()
        complete_setup()

    project_uuid = get_project_uuid()
    space_uuid = get_shared_space_uuid(project_uuid)

    # --- Auto-discover and sync charts from /dashboard/charts/*.json ---
    chart_configs = discover_chart_configs()
    if chart_configs:
        charts_resp = api("GET", f"/api/v1/projects/{project_uuid}/charts")
        existing_charts = charts_resp.get("results", []) if charts_resp.get("status") == "ok" else []
        for chart_def in chart_configs:
            ensure_chart(project_uuid, space_uuid, chart_def, existing_charts)
    else:
        log("No chart configs found.")

    # --- Auto-discover and sync dashboards from /dashboard/*.json ---
    dashboard_configs = discover_dashboard_configs()
    if dashboard_configs:
        dashboards_resp = api("GET", f"/api/v1/projects/{project_uuid}/dashboards")
        existing_dashboards = dashboards_resp.get("results", []) if dashboards_resp.get("status") == "ok" else []
        for dash_def in dashboard_configs:
            ensure_dashboard(project_uuid, space_uuid, dash_def, existing_dashboards)
    else:
        log("No dashboard configs found.")

    dismiss_onboarding(project_uuid)

    log("=" * 50)
    log("  Lightdash is ready!")
    log(f"  Synced {len(chart_configs)} chart(s), {len(dashboard_configs)} dashboard(s).")
    log(f"  Email:    {ADMIN_EMAIL}")
    log(f"  Password: {ADMIN_PASSWORD}")
    log("=" * 50)


if __name__ == "__main__":
    main()
