"""
Jobber Activity Log — main.py

Polls the Jobber GraphQL API every 2 minutes for changes to quotes, jobs,
invoices, and visits. Stores events in SQLite and serves a live dashboard
with CSV export. Retroactively loads all activity since midnight on startup.
"""

import csv
import hashlib
import hmac
import io
import json
import os
import secrets
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from fastapi import FastAPI, Form, Request, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from jinja2 import Environment, select_autoescape

# ── SECTION 1: CONFIGURATION ────────────────────────────────────────────────────

CENTRAL_TZ = ZoneInfo("America/Chicago")
POLL_INTERVAL = 120  # seconds between poll cycles

# SQLite path — set DB_DIR env var and mount a Railway volume at /data for persistence
_db_dir = os.environ.get("DB_DIR", "/data")
if not os.path.isdir(_db_dir):
    _db_dir = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_db_dir, "activity_log.db")


def _midnight_central_utc() -> datetime:
    """Return midnight Central time from last night as a UTC datetime."""
    now_central = datetime.now(CENTRAL_TZ)
    midnight_central = now_central.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight_central.astimezone(timezone.utc)


# ── SECTION 2: DATABASE ─────────────────────────────────────────────────────────

_db_lock = threading.Lock()


def init_db() -> None:
    with _db_lock, sqlite3.connect(DB_PATH) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS activity_log (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                logged_at    TEXT NOT NULL,
                event_time   TEXT NOT NULL,
                entity_type  TEXT NOT NULL,
                entity_id    TEXT NOT NULL,
                entity_ref   TEXT,
                action       TEXT NOT NULL,
                detail       TEXT,
                client_name  TEXT,
                employee     TEXT,
                action_by    TEXT,
                old_value    TEXT,
                new_value    TEXT
            );

            CREATE TABLE IF NOT EXISTS entity_state (
                entity_type  TEXT NOT NULL,
                entity_id    TEXT NOT NULL,
                state_json   TEXT NOT NULL,
                updated_at   TEXT NOT NULL,
                PRIMARY KEY (entity_type, entity_id)
            );
        """)
        # Migration: add action_by column if it doesn't exist yet (for existing DBs)
        existing = {row[1] for row in conn.execute("PRAGMA table_info(activity_log)").fetchall()}
        if "action_by" not in existing:
            conn.execute("ALTER TABLE activity_log ADD COLUMN action_by TEXT")


def insert_event(
    event_time: str,
    entity_type: str,
    entity_id: str,
    entity_ref: str,
    action: str,
    detail: str = "",
    client_name: str = "",
    employee: str = "",
    action_by: str = "",
    old_value: str = "",
    new_value: str = "",
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _db_lock, sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """INSERT INTO activity_log
               (logged_at, event_time, entity_type, entity_id, entity_ref,
                action, detail, client_name, employee, action_by, old_value, new_value)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (now, event_time, entity_type, entity_id, entity_ref,
             action, detail, client_name, employee, action_by, old_value, new_value),
        )


def get_events(limit: int = 1000) -> list[dict]:
    with _db_lock, sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM activity_log ORDER BY event_time DESC, id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_all_events_for_csv() -> list[dict]:
    with _db_lock, sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT event_time, entity_type, entity_ref, action, detail,
                      client_name, employee, action_by, old_value, new_value, logged_at
               FROM activity_log ORDER BY event_time DESC, id DESC"""
        ).fetchall()
    return [dict(r) for r in rows]


def get_entity_state(entity_type: str, entity_id: str) -> Optional[dict]:
    with _db_lock, sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT state_json, updated_at FROM entity_state WHERE entity_type=? AND entity_id=?",
            (entity_type, entity_id),
        ).fetchone()
    if row is None:
        return None
    return {"state": json.loads(row[0]), "updated_at": row[1]}


def upsert_entity_state(entity_type: str, entity_id: str, state: dict, updated_at: str) -> None:
    with _db_lock, sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """INSERT INTO entity_state (entity_type, entity_id, state_json, updated_at)
               VALUES (?,?,?,?)
               ON CONFLICT(entity_type, entity_id) DO UPDATE SET
                 state_json=excluded.state_json,
                 updated_at=excluded.updated_at""",
            (entity_type, entity_id, json.dumps(state), updated_at),
        )


# ── SECTION 3: JOBBER API LAYER ─────────────────────────────────────────────────

JOBBER_GRAPHQL_URL = "https://api.getjobber.com/api/graphql"
JOBBER_GRAPHQL_VERSION = "2025-04-16"
JOBBER_TOKEN_URL = "https://api.getjobber.com/api/oauth/token"

# Validated against Jobber GraphQL schema 2025-04-16.
# Key notes:
#   - Job has no assignedUsers field (team assignment lives on Visit)
#   - Quote total is nested under amounts { total }
#   - Visit has no updatedAt field
#   - All three paginated types sort by updatedAt DESC (confirmed via introspection)

JOBS_QUERY = """
query Jobs($first: Int!, $after: String) {
  jobs(first: $first, after: $after) {
    nodes {
      id
      jobNumber
      title
      jobStatus
      createdAt
      updatedAt
      total
      client {
        firstName
        lastName
        companyName
        isCompany
      }
      notes(first: 10) {
        nodes {
          id
          content
          createdAt
          updatedAt
          createdBy { id name { full } }
          lastEditedBy { id name { full } }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

QUOTES_QUERY = """
query Quotes($first: Int!, $after: String) {
  quotes(first: $first, after: $after) {
    nodes {
      id
      quoteNumber
      title
      quoteStatus
      createdAt
      updatedAt
      amounts { total }
      client {
        firstName
        lastName
        companyName
        isCompany
      }
      notes(first: 10) {
        nodes {
          id
          content
          createdAt
          updatedAt
          createdBy { id name { full } }
          lastEditedBy { id name { full } }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

INVOICES_QUERY = """
query Invoices($first: Int!, $after: String) {
  invoices(first: $first, after: $after) {
    nodes {
      id
      invoiceNumber
      invoiceStatus
      createdAt
      updatedAt
      total
      client {
        firstName
        lastName
        companyName
        isCompany
      }
      notes(first: 10) {
        nodes {
          id
          content
          createdAt
          updatedAt
          createdBy { id name { full } }
          lastEditedBy { id name { full } }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

VISITS_QUERY = """
query VisitsActivity($filter: VisitFilterAttributes!, $first: Int!, $after: String) {
  visits(filter: $filter, first: $first, after: $after) {
    nodes {
      id
      startAt
      endAt
      allDay
      createdAt
      visitStatus
      title
      client {
        firstName
        lastName
        companyName
        isCompany
      }
      job {
        jobNumber
        title
      }
      assignedUsers(first: 10) {
        nodes {
          id
          name { full }
        }
      }
      createdBy { id name { full } }
      completedBy { id name { full } }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

EXPENSES_QUERY = """
query Expenses($first: Int!, $after: String) {
  expenses(first: $first, after: $after) {
    nodes {
      id
      description
      total
      createdAt
      updatedAt
      enteredBy { id name { full } }
      paidBy { id name { full } }
      job { jobNumber }
      client {
        firstName
        lastName
        companyName
        isCompany
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

TIMESHEETS_QUERY = """
query TimesheetEntries($first: Int!, $after: String) {
  timesheetEntries(first: $first, after: $after) {
    nodes {
      id
      startAt
      endAt
      createdAt
      updatedAt
      approvedAt
      approvedBy { id name { full } }
      user { id name { full } }
      job { jobNumber }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""


def refresh_access_token() -> str:
    """Exchange the current refresh token for a fresh access token.

    The Jobber refresh token rotates on every use. The new token is printed
    to logs so it can be manually updated in Railway environment variables.
    """
    response = requests.post(
        JOBBER_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": os.environ["JOBBER_REFRESH_TOKEN"],
            "client_id": os.environ["JOBBER_CLIENT_ID"],
            "client_secret": os.environ["JOBBER_CLIENT_SECRET"],
        },
    )
    response.raise_for_status()
    data = response.json()
    new_refresh_token = data["refresh_token"]
    access_token = data["access_token"]
    os.environ["JOBBER_REFRESH_TOKEN"] = new_refresh_token
    print(f"[TOKEN ROTATION] New JOBBER_REFRESH_TOKEN: {new_refresh_token}", flush=True)
    return access_token


def _run_query(access_token: str, query: str, variables: dict) -> dict:
    response = requests.post(
        JOBBER_GRAPHQL_URL,
        headers={
            "Authorization": f"Bearer {access_token}",
            "X-JOBBER-GRAPHQL-VERSION": JOBBER_GRAPHQL_VERSION,
            "Content-Type": "application/json",
        },
        json={"query": query, "variables": variables},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if "errors" in payload:
        raise RuntimeError(f"Jobber GraphQL errors: {payload['errors']}")
    return payload["data"]


def _fetch_paginated(
    access_token: str,
    query: str,
    data_key: str,
    since_iso: str,
    page_size: int = 50,
    max_pages: int = 20,
) -> list[dict]:
    """Page through a Jobber connection and return items updated since since_iso.

    Jobber returns jobs/quotes/invoices sorted by updatedAt DESC (confirmed via
    introspection). We stop as soon as the last item on a page is older than
    since_iso — typically 1–2 pages for a day's activity. A 0.2s sleep between
    pages keeps us comfortably below Jobber's rate limits.
    """
    nodes: list[dict] = []
    after = None

    for _ in range(max_pages):
        variables: dict = {"first": page_size}
        if after:
            variables["after"] = after

        data = _run_query(access_token, query, variables)
        connection = data[data_key]
        page_nodes = connection["nodes"]
        page_info = connection["pageInfo"]

        if not page_nodes:
            break

        for node in page_nodes:
            node_time = node.get("updatedAt") or node.get("createdAt") or ""
            if node_time >= since_iso:
                nodes.append(node)

        # Stop early: last item is older than our window (list is updatedAt DESC)
        last_time = page_nodes[-1].get("updatedAt") or page_nodes[-1].get("createdAt") or ""
        if last_time < since_iso or not page_info.get("hasNextPage"):
            break

        after = page_info.get("endCursor")
        time.sleep(0.2)

    return nodes


def fetch_recent_jobs(access_token: str, since_iso: str) -> list[dict]:
    return _fetch_paginated(access_token, JOBS_QUERY, "jobs", since_iso)


def fetch_recent_quotes(access_token: str, since_iso: str) -> list[dict]:
    return _fetch_paginated(access_token, QUOTES_QUERY, "quotes", since_iso)


def fetch_recent_invoices(access_token: str, since_iso: str) -> list[dict]:
    return _fetch_paginated(access_token, INVOICES_QUERY, "invoices", since_iso)


def fetch_recent_expenses(access_token: str, since_iso: str) -> list[dict]:
    return _fetch_paginated(access_token, EXPENSES_QUERY, "expenses", since_iso)


def fetch_recent_timesheets(access_token: str, since_iso: str) -> list[dict]:
    return _fetch_paginated(access_token, TIMESHEETS_QUERY, "timesheetEntries", since_iso)


def fetch_single_job(access_token: str, job_id: str) -> Optional[dict]:
    """Fetch a single job by ID for webhook-triggered updates."""
    query = """
    query Job($id: EncodedId!) {
      job(id: $id) {
        id jobNumber title jobStatus createdAt updatedAt total
        client { firstName lastName companyName isCompany }
        notes(first: 10) {
          nodes {
            id content createdAt updatedAt
            createdBy { id name { full } }
            lastEditedBy { id name { full } }
          }
        }
      }
    }
    """
    try:
        data = _run_query(access_token, query, {"id": job_id})
        return data.get("job")
    except Exception as exc:
        print(f"[WEBHOOK] fetch_single_job {job_id}: {exc}", flush=True)
        return None


def fetch_single_quote(access_token: str, quote_id: str) -> Optional[dict]:
    """Fetch a single quote by ID for webhook-triggered updates."""
    query = """
    query Quote($id: EncodedId!) {
      quote(id: $id) {
        id quoteNumber title quoteStatus createdAt updatedAt
        amounts { total }
        client { firstName lastName companyName isCompany }
        notes(first: 10) {
          nodes {
            id content createdAt updatedAt
            createdBy { id name { full } }
            lastEditedBy { id name { full } }
          }
        }
      }
    }
    """
    try:
        data = _run_query(access_token, query, {"id": quote_id})
        return data.get("quote")
    except Exception as exc:
        print(f"[WEBHOOK] fetch_single_quote {quote_id}: {exc}", flush=True)
        return None


def fetch_single_invoice(access_token: str, invoice_id: str) -> Optional[dict]:
    """Fetch a single invoice by ID for webhook-triggered updates."""
    query = """
    query Invoice($id: EncodedId!) {
      invoice(id: $id) {
        id invoiceNumber invoiceStatus createdAt updatedAt total
        client { firstName lastName companyName isCompany }
        notes(first: 10) {
          nodes {
            id content createdAt updatedAt
            createdBy { id name { full } }
            lastEditedBy { id name { full } }
          }
        }
      }
    }
    """
    try:
        data = _run_query(access_token, query, {"id": invoice_id})
        return data.get("invoice")
    except Exception as exc:
        print(f"[WEBHOOK] fetch_single_invoice {invoice_id}: {exc}", flush=True)
        return None


def fetch_single_visit(access_token: str, visit_id: str) -> Optional[dict]:
    """Fetch a single visit by ID for webhook-triggered updates."""
    query = """
    query Visit($id: EncodedId!) {
      visit(id: $id) {
        id startAt endAt allDay createdAt visitStatus title
        client { firstName lastName companyName isCompany }
        job { jobNumber title }
        assignedUsers(first: 10) { nodes { id name { full } } }
        createdBy { id name { full } }
        completedBy { id name { full } }
      }
    }
    """
    try:
        data = _run_query(access_token, query, {"id": visit_id})
        return data.get("visit")
    except Exception as exc:
        print(f"[WEBHOOK] fetch_single_visit {visit_id}: {exc}", flush=True)
        return None


def fetch_recent_visits(access_token: str) -> list[dict]:
    """Fetch all visits in the active scheduling window (past 30 days to +60 days ahead).

    Returns ALL visits in the window — no date filter applied here. The change
    detection logic in process_visit decides what to log based on stored state.
    Visits have no updatedAt field, so we must compare state on every cycle.
    """
    window_start = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    window_end = (datetime.now(timezone.utc) + timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    base_filter = {"startAt": {"after": window_start, "before": window_end}}

    nodes: list[dict] = []
    after = None
    for _ in range(20):  # Max 1,000 visits
        variables = {"filter": base_filter, "first": 50}
        if after:
            variables["after"] = after
        try:
            data = _run_query(access_token, VISITS_QUERY, variables)
        except Exception as exc:
            print(f"[VISITS QUERY ERROR] {exc}", flush=True)
            break
        page_nodes = data["visits"]["nodes"]
        page_info = data["visits"]["pageInfo"]
        nodes.extend(page_nodes)
        if not page_info.get("hasNextPage"):
            break
        after = page_info.get("endCursor")
        time.sleep(0.2)

    return nodes


# ── SECTION 4: CHANGE DETECTION ─────────────────────────────────────────────────

def _user_name(user: Optional[dict]) -> str:
    """Extract the full name from a Jobber User object."""
    if not user:
        return ""
    try:
        return user["name"]["full"]
    except (KeyError, TypeError):
        return ""


def _client_name(client: Optional[dict]) -> str:
    if not client:
        return "Unknown"
    if client.get("isCompany") and client.get("companyName"):
        return client["companyName"]
    first = client.get("firstName") or ""
    last = client.get("lastName") or ""
    return f"{first} {last}".strip() or "Unknown"


def _assigned_users(node: dict) -> list[str]:
    try:
        return [u["name"]["full"] for u in node["assignedUsers"]["nodes"]]
    except (KeyError, TypeError):
        return []


def _fmt_status(status: str) -> str:
    return status.replace("_", " ").title() if status else ""


def _fmt_currency(val) -> str:
    try:
        return f"${float(val):,.2f}"
    except (TypeError, ValueError):
        return str(val) if val else ""


def _fmt_dt_central(iso: str) -> str:
    """Format an ISO UTC timestamp as Central time for display."""
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(CENTRAL_TZ)
        hour = dt.hour % 12 or 12
        ampm = "AM" if dt.hour < 12 else "PM"
        return f"{dt.strftime('%b')} {dt.day}, {dt.year} {hour}:{dt.strftime('%M')} {ampm} CT"
    except Exception:
        return iso


def process_job(node: dict, since_iso: str) -> None:
    """Detect changes on a job node and insert activity events.

    Jobs in Jobber's API do not expose an assignedUsers field — team assignment
    lives on individual visits. Employee column is left blank for job events.
    """
    eid = node["id"]
    ref = f"Job #{node.get('jobNumber', '?')}"
    client = _client_name(node.get("client"))
    event_time = node.get("updatedAt") or node.get("createdAt") or ""
    created_at = node.get("createdAt") or ""
    total = node.get("total")
    total_str = str(total) if total is not None else ""

    stored = get_entity_state("job", eid)

    # Snapshot of current state for storage
    current_state = {
        "jobStatus": node.get("jobStatus", ""),
        "title": node.get("title", ""),
        "total": total_str,
        "updatedAt": event_time,
    }

    if stored is None:
        # First time seeing this entity
        if created_at >= since_iso:
            insert_event(
                event_time=created_at,
                entity_type="job",
                entity_id=eid,
                entity_ref=ref,
                action="Created",
                detail=f"Status: {_fmt_status(node.get('jobStatus', ''))} | Total: {_fmt_currency(total)}",
                client_name=client,
            )
        elif event_time >= since_iso:
            insert_event(
                event_time=event_time,
                entity_type="job",
                entity_id=eid,
                entity_ref=ref,
                action="Updated",
                detail=f"Status: {_fmt_status(node.get('jobStatus', ''))}",
                client_name=client,
            )
        note_state = _process_notes(node.get("notes"), "job", eid, ref, client, {})
        current_state.update(note_state)
        upsert_entity_state("job", eid, current_state, event_time)
        return

    prev = stored["state"]
    prev_updated = stored["updated_at"]

    if event_time and prev_updated and event_time <= prev_updated:
        # Still check for new notes even if nothing else changed
        note_state = _process_notes(node.get("notes"), "job", eid, ref, client, prev)
        if note_state != {k: prev.get(k) for k in ("note_ids", "note_updated_ats")}:
            current_state.update(note_state)
            upsert_entity_state("job", eid, current_state, event_time)
        return

    if prev.get("jobStatus") != current_state["jobStatus"]:
        insert_event(
            event_time=event_time,
            entity_type="job",
            entity_id=eid,
            entity_ref=ref,
            action="Status Changed",
            detail=f"{_fmt_status(prev.get('jobStatus', ''))} → {_fmt_status(current_state['jobStatus'])}",
            client_name=client,
            old_value=_fmt_status(prev.get("jobStatus", "")),
            new_value=_fmt_status(current_state["jobStatus"]),
        )

    if prev.get("title") != current_state["title"] and current_state["title"]:
        insert_event(
            event_time=event_time,
            entity_type="job",
            entity_id=eid,
            entity_ref=ref,
            action="Title Updated",
            detail=f'"{current_state["title"]}"',
            client_name=client,
            old_value=prev.get("title", ""),
            new_value=current_state["title"],
        )

    if prev.get("total") != current_state["total"] and current_state["total"]:
        insert_event(
            event_time=event_time,
            entity_type="job",
            entity_id=eid,
            entity_ref=ref,
            action="Total Updated",
            detail=f"{_fmt_currency(prev.get('total'))} → {_fmt_currency(total)}",
            client_name=client,
            old_value=_fmt_currency(prev.get("total")),
            new_value=_fmt_currency(total),
        )

    if (
        event_time != prev_updated
        and prev.get("jobStatus") == current_state["jobStatus"]
        and prev.get("title") == current_state["title"]
        and prev.get("total") == current_state["total"]
    ):
        insert_event(
            event_time=event_time,
            entity_type="job",
            entity_id=eid,
            entity_ref=ref,
            action="Updated",
            detail="Details edited",
            client_name=client,
        )

    note_state = _process_notes(node.get("notes"), "job", eid, ref, client, prev)
    current_state.update(note_state)
    upsert_entity_state("job", eid, current_state, event_time)


def process_quote(node: dict, since_iso: str) -> None:
    eid = node["id"]
    ref = f"Quote #{node.get('quoteNumber', '?')}"
    client = _client_name(node.get("client"))
    event_time = node.get("updatedAt") or node.get("createdAt") or ""
    created_at = node.get("createdAt") or ""
    # Quote total is nested: amounts.total (not a top-level field)
    total = (node.get("amounts") or {}).get("total")
    total_str = str(total) if total is not None else ""

    stored = get_entity_state("quote", eid)

    current_state = {
        "quoteStatus": node.get("quoteStatus", ""),
        "title": node.get("title", ""),
        "total": total_str,
        "updatedAt": event_time,
    }

    if stored is None:
        if created_at >= since_iso:
            insert_event(
                event_time=created_at,
                entity_type="quote",
                entity_id=eid,
                entity_ref=ref,
                action="Created",
                detail=f"Status: {_fmt_status(node.get('quoteStatus', ''))} | Total: {_fmt_currency(total)}",
                client_name=client,
            )
        elif event_time >= since_iso:
            insert_event(
                event_time=event_time,
                entity_type="quote",
                entity_id=eid,
                entity_ref=ref,
                action="Updated",
                detail=f"Status: {_fmt_status(node.get('quoteStatus', ''))} | Total: {_fmt_currency(total)}",
                client_name=client,
            )
        note_state = _process_notes(node.get("notes"), "quote", eid, ref, client, {})
        current_state.update(note_state)
        upsert_entity_state("quote", eid, current_state, event_time)
        return

    prev = stored["state"]
    prev_updated = stored["updated_at"]

    if event_time and prev_updated and event_time <= prev_updated:
        note_state = _process_notes(node.get("notes"), "quote", eid, ref, client, prev)
        if note_state != {k: prev.get(k) for k in ("note_ids", "note_updated_ats")}:
            current_state.update(note_state)
            upsert_entity_state("quote", eid, current_state, event_time)
        return

    if prev.get("quoteStatus") != current_state["quoteStatus"]:
        insert_event(
            event_time=event_time,
            entity_type="quote",
            entity_id=eid,
            entity_ref=ref,
            action="Status Changed",
            detail=f"{_fmt_status(prev.get('quoteStatus', ''))} → {_fmt_status(current_state['quoteStatus'])}",
            client_name=client,
            old_value=_fmt_status(prev.get("quoteStatus", "")),
            new_value=_fmt_status(current_state["quoteStatus"]),
        )

    if prev.get("total") != current_state["total"] and current_state["total"]:
        insert_event(
            event_time=event_time,
            entity_type="quote",
            entity_id=eid,
            entity_ref=ref,
            action="Total Updated",
            detail=f"{_fmt_currency(prev.get('total'))} → {_fmt_currency(total)}",
            client_name=client,
            old_value=_fmt_currency(prev.get("total")),
            new_value=_fmt_currency(total),
        )

    if prev.get("title") != current_state["title"] and current_state["title"]:
        insert_event(
            event_time=event_time,
            entity_type="quote",
            entity_id=eid,
            entity_ref=ref,
            action="Title Updated",
            detail=f'"{current_state["title"]}"',
            client_name=client,
            old_value=prev.get("title", ""),
            new_value=current_state["title"],
        )

    if (
        event_time != prev_updated
        and prev.get("quoteStatus") == current_state["quoteStatus"]
        and prev.get("total") == current_state["total"]
        and prev.get("title") == current_state["title"]
    ):
        insert_event(
            event_time=event_time,
            entity_type="quote",
            entity_id=eid,
            entity_ref=ref,
            action="Updated",
            detail="Details edited",
            client_name=client,
        )

    note_state = _process_notes(node.get("notes"), "quote", eid, ref, client, prev)
    current_state.update(note_state)
    upsert_entity_state("quote", eid, current_state, event_time)


def process_invoice(node: dict, since_iso: str) -> None:
    eid = node["id"]
    ref = f"Invoice #{node.get('invoiceNumber', '?')}"
    client = _client_name(node.get("client"))
    event_time = node.get("updatedAt") or node.get("createdAt") or ""
    created_at = node.get("createdAt") or ""

    stored = get_entity_state("invoice", eid)

    current_state = {
        "invoiceStatus": node.get("invoiceStatus", ""),
        "total": str(node.get("total", "")),
        "updatedAt": event_time,
    }

    if stored is None:
        if created_at >= since_iso:
            insert_event(
                event_time=created_at,
                entity_type="invoice",
                entity_id=eid,
                entity_ref=ref,
                action="Created",
                detail=f"Status: {_fmt_status(node.get('invoiceStatus', ''))} | Total: {_fmt_currency(node.get('total'))}",
                client_name=client,
            )
        elif event_time >= since_iso:
            insert_event(
                event_time=event_time,
                entity_type="invoice",
                entity_id=eid,
                entity_ref=ref,
                action="Updated",
                detail=f"Status: {_fmt_status(node.get('invoiceStatus', ''))} | Total: {_fmt_currency(node.get('total'))}",
                client_name=client,
            )
        note_state = _process_notes(node.get("notes"), "invoice", eid, ref, client, {})
        current_state.update(note_state)
        upsert_entity_state("invoice", eid, current_state, event_time)
        return

    prev = stored["state"]
    prev_updated = stored["updated_at"]

    if event_time and prev_updated and event_time <= prev_updated:
        note_state = _process_notes(node.get("notes"), "invoice", eid, ref, client, prev)
        if note_state != {k: prev.get(k) for k in ("note_ids", "note_updated_ats")}:
            current_state.update(note_state)
            upsert_entity_state("invoice", eid, current_state, event_time)
        return

    if prev.get("invoiceStatus") != current_state["invoiceStatus"]:
        insert_event(
            event_time=event_time,
            entity_type="invoice",
            entity_id=eid,
            entity_ref=ref,
            action="Status Changed",
            detail=f"{_fmt_status(prev.get('invoiceStatus', ''))} → {_fmt_status(current_state['invoiceStatus'])}",
            client_name=client,
            old_value=_fmt_status(prev.get("invoiceStatus", "")),
            new_value=_fmt_status(current_state["invoiceStatus"]),
        )

    if prev.get("total") != current_state["total"] and current_state["total"]:
        insert_event(
            event_time=event_time,
            entity_type="invoice",
            entity_id=eid,
            entity_ref=ref,
            action="Total Updated",
            detail=f"{_fmt_currency(prev.get('total'))} → {_fmt_currency(node.get('total'))}",
            client_name=client,
            old_value=_fmt_currency(prev.get("total")),
            new_value=_fmt_currency(node.get("total")),
        )

    if (
        event_time != prev_updated
        and prev.get("invoiceStatus") == current_state["invoiceStatus"]
        and prev.get("total") == current_state["total"]
    ):
        insert_event(
            event_time=event_time,
            entity_type="invoice",
            entity_id=eid,
            entity_ref=ref,
            action="Updated",
            detail="Details edited",
            client_name=client,
        )

    note_state = _process_notes(node.get("notes"), "invoice", eid, ref, client, prev)
    current_state.update(note_state)
    upsert_entity_state("invoice", eid, current_state, event_time)


def _process_notes(
    notes_connection: Optional[dict],
    entity_type: str,
    entity_id: str,
    entity_ref: str,
    client: str,
    stored_state: dict,
) -> dict:
    """Detect new or edited notes on a job/quote/invoice. Returns updated note state."""
    nodes = []
    try:
        nodes = (notes_connection or {}).get("nodes", [])
    except (AttributeError, TypeError):
        pass

    prev_note_ids: set = set(stored_state.get("note_ids", []))
    prev_note_updated: dict = stored_state.get("note_updated_ats", {})

    current_note_ids = []
    current_note_updated = {}

    for note in nodes:
        nid = note.get("id", "")
        if not nid:
            continue
        note_updated = note.get("updatedAt") or note.get("createdAt") or ""
        note_created = note.get("createdAt") or ""
        current_note_ids.append(nid)
        current_note_updated[nid] = note_updated

        content = (note.get("content") or "")[:120]
        created_by = _user_name(note.get("createdBy"))
        last_edited_by = _user_name(note.get("lastEditedBy"))

        if nid not in prev_note_ids:
            # Brand new note
            insert_event(
                event_time=note_created,
                entity_type=entity_type,
                entity_id=entity_id,
                entity_ref=entity_ref,
                action="Note Added",
                detail=content,
                client_name=client,
                action_by=created_by,
            )
        elif prev_note_updated.get(nid) and note_updated > prev_note_updated[nid]:
            # Existing note was edited
            editor = last_edited_by or created_by
            insert_event(
                event_time=note_updated,
                entity_type=entity_type,
                entity_id=entity_id,
                entity_ref=entity_ref,
                action="Note Edited",
                detail=content,
                client_name=client,
                action_by=editor,
            )

    return {"note_ids": current_note_ids, "note_updated_ats": current_note_updated}


def process_visit(node: dict, since_iso: str) -> None:
    eid = node["id"]
    job = node.get("job") or {}
    job_num = job.get("jobNumber", "?")
    ref = f"Visit (Job #{job_num})"
    client = _client_name(node.get("client"))
    users = _assigned_users(node)
    employee = ", ".join(users) if users else "Unassigned"
    # Visits have no updatedAt — use startAt as the display timestamp for events
    start_at = node.get("startAt", "")
    created_at = node.get("createdAt") or ""
    now_iso = datetime.now(timezone.utc).isoformat()
    visit_status = node.get("visitStatus", "")
    created_by = _user_name(node.get("createdBy"))
    completed_by = _user_name(node.get("completedBy"))

    stored = get_entity_state("visit", eid)

    # State snapshot — used for change detection (no updatedAt available)
    current_state = {
        "startAt": start_at,
        "endAt": node.get("endAt", ""),
        "allDay": node.get("allDay", False),
        "assignedUsers": sorted(users),
        "visitStatus": visit_status,
        "completedBy": completed_by,
    }

    if stored is None:
        # First time seeing this visit
        start_fmt = _fmt_dt_central(start_at)
        if created_at >= since_iso:
            insert_event(
                event_time=created_at,
                entity_type="visit",
                entity_id=eid,
                entity_ref=ref,
                action="Created",
                detail=f"Scheduled: {start_fmt}",
                client_name=client,
                employee=employee,
                action_by=created_by,
            )
            if visit_status == "COMPLETED":
                insert_event(
                    event_time=now_iso,
                    entity_type="visit",
                    entity_id=eid,
                    entity_ref=ref,
                    action="Completed",
                    detail=f"Completed by: {completed_by}" if completed_by else "Visit marked complete",
                    client_name=client,
                    employee=employee,
                    action_by=completed_by,
                )
        # For pre-existing visits, just store state silently (no change to report)
        upsert_entity_state("visit", eid, current_state, now_iso)
        return

    prev = stored["state"]

    # Compare state — since there's no updatedAt, we always compare
    if prev.get("startAt") != current_state["startAt"]:
        old_fmt = _fmt_dt_central(prev.get("startAt", ""))
        new_fmt = _fmt_dt_central(current_state["startAt"])
        insert_event(
            event_time=now_iso,
            entity_type="visit",
            entity_id=eid,
            entity_ref=ref,
            action="Visit Rescheduled",
            detail=f"{old_fmt} → {new_fmt}",
            client_name=client,
            employee=employee,
            old_value=old_fmt,
            new_value=new_fmt,
        )

    if prev.get("endAt") != current_state["endAt"]:
        old_fmt = _fmt_dt_central(prev.get("endAt", ""))
        new_fmt = _fmt_dt_central(current_state["endAt"])
        insert_event(
            event_time=now_iso,
            entity_type="visit",
            entity_id=eid,
            entity_ref=ref,
            action="Visit Duration Changed",
            detail=f"End: {old_fmt} → {new_fmt}",
            client_name=client,
            employee=employee,
            old_value=old_fmt,
            new_value=new_fmt,
        )

    if prev.get("assignedUsers", []) != current_state["assignedUsers"]:
        old_u = ", ".join(prev.get("assignedUsers", [])) or "Unassigned"
        new_u = ", ".join(current_state["assignedUsers"]) or "Unassigned"
        insert_event(
            event_time=now_iso,
            entity_type="visit",
            entity_id=eid,
            entity_ref=ref,
            action="Team Assignment Changed",
            detail=f"{old_u} → {new_u}",
            client_name=client,
            employee=employee,
            old_value=old_u,
            new_value=new_u,
        )

    # Detect newly completed visits
    if prev.get("visitStatus") != "COMPLETED" and visit_status == "COMPLETED":
        insert_event(
            event_time=now_iso,
            entity_type="visit",
            entity_id=eid,
            entity_ref=ref,
            action="Completed",
            detail=f"Completed by: {completed_by}" if completed_by else "Visit marked complete",
            client_name=client,
            employee=employee,
            action_by=completed_by,
        )

    upsert_entity_state("visit", eid, current_state, now_iso)


def process_expense(node: dict, since_iso: str) -> None:
    eid = node["id"]
    job = node.get("job") or {}
    job_num = job.get("jobNumber")
    ref = f"Expense (Job #{job_num})" if job_num else "Expense"
    client = _client_name(node.get("client"))
    event_time = node.get("updatedAt") or node.get("createdAt") or ""
    created_at = node.get("createdAt") or ""
    entered_by = _user_name(node.get("enteredBy"))
    paid_by = _user_name(node.get("paidBy"))
    total = node.get("total")
    description = (node.get("description") or "")[:80]

    stored = get_entity_state("expense", eid)

    current_state = {
        "total": str(total) if total is not None else "",
        "enteredBy": entered_by,
        "paidBy": paid_by,
        "updatedAt": event_time,
    }

    if stored is None:
        if created_at >= since_iso:
            insert_event(
                event_time=created_at,
                entity_type="expense",
                entity_id=eid,
                entity_ref=ref,
                action="Expense Entered",
                detail=f"{description} | {_fmt_currency(total)}" if description else _fmt_currency(total),
                client_name=client,
                action_by=entered_by,
            )
        upsert_entity_state("expense", eid, current_state, event_time)
        return

    prev = stored["state"]
    prev_updated = stored["updated_at"]

    if event_time and prev_updated and event_time <= prev_updated:
        return

    # Detect when an expense gets marked paid (paidBy appears)
    if not prev.get("paidBy") and paid_by:
        insert_event(
            event_time=event_time,
            entity_type="expense",
            entity_id=eid,
            entity_ref=ref,
            action="Expense Paid",
            detail=f"{description} | {_fmt_currency(total)}" if description else _fmt_currency(total),
            client_name=client,
            action_by=paid_by,
        )

    upsert_entity_state("expense", eid, current_state, event_time)


def process_timesheet(node: dict, since_iso: str) -> None:
    eid = node["id"]
    job = node.get("job") or {}
    job_num = job.get("jobNumber")
    ref = f"Timesheet (Job #{job_num})" if job_num else "Timesheet"
    event_time = node.get("updatedAt") or node.get("createdAt") or ""
    created_at = node.get("createdAt") or ""
    worker = _user_name(node.get("user"))
    approved_by = _user_name(node.get("approvedBy"))
    start_at = node.get("startAt", "")
    approved_at = node.get("approvedAt") or ""

    stored = get_entity_state("timesheet", eid)

    current_state = {
        "approvedBy": approved_by,
        "approvedAt": approved_at,
        "updatedAt": event_time,
    }

    if stored is None:
        if created_at >= since_iso:
            start_fmt = _fmt_dt_central(start_at)
            insert_event(
                event_time=created_at,
                entity_type="timesheet",
                entity_id=eid,
                entity_ref=ref,
                action="Time Logged",
                detail=f"Worker: {worker} | Start: {start_fmt}" if worker else f"Start: {start_fmt}",
                employee=worker,
            )
        upsert_entity_state("timesheet", eid, current_state, event_time)
        return

    prev = stored["state"]
    prev_updated = stored["updated_at"]

    if event_time and prev_updated and event_time <= prev_updated:
        return

    # Detect approval
    if not prev.get("approvedBy") and approved_by:
        insert_event(
            event_time=approved_at or event_time,
            entity_type="timesheet",
            entity_id=eid,
            entity_ref=ref,
            action="Timesheet Approved",
            detail=f"Worker: {worker}" if worker else "",
            employee=worker,
            action_by=approved_by,
        )

    upsert_entity_state("timesheet", eid, current_state, event_time)


# ── SECTION 5: POLLING ENGINE ────────────────────────────────────────────────────

_last_error: str = ""
_last_poll_time: str = ""


def run_poll_cycle(since_utc: datetime) -> None:
    """Fetch recent changes from Jobber and insert activity events."""
    global _last_error, _last_poll_time

    since_iso = since_utc.isoformat()
    print(f"[POLL] Starting cycle since {since_iso}", flush=True)

    try:
        token = refresh_access_token()
    except Exception as exc:
        _last_error = f"Token refresh failed: {exc}"
        print(f"[POLL ERROR] {_last_error}", flush=True)
        return

    # Jobs
    try:
        jobs = fetch_recent_jobs(token, since_iso)
        print(f"[POLL] {len(jobs)} jobs to process", flush=True)
        for node in jobs:
            try:
                process_job(node, since_iso)
            except Exception as exc:
                print(f"[JOB ERROR] {node.get('id')}: {exc}", flush=True)
    except Exception as exc:
        print(f"[POLL JOBS ERROR] {exc}", flush=True)

    # Quotes
    try:
        quotes = fetch_recent_quotes(token, since_iso)
        print(f"[POLL] {len(quotes)} quotes to process", flush=True)
        for node in quotes:
            try:
                process_quote(node, since_iso)
            except Exception as exc:
                print(f"[QUOTE ERROR] {node.get('id')}: {exc}", flush=True)
    except Exception as exc:
        print(f"[POLL QUOTES ERROR] {exc}", flush=True)

    # Invoices
    try:
        invoices = fetch_recent_invoices(token, since_iso)
        print(f"[POLL] {len(invoices)} invoices to process", flush=True)
        for node in invoices:
            try:
                process_invoice(node, since_iso)
            except Exception as exc:
                print(f"[INVOICE ERROR] {node.get('id')}: {exc}", flush=True)
    except Exception as exc:
        print(f"[POLL INVOICES ERROR] {exc}", flush=True)

    # Visits
    # Visits: fetch all in active window every cycle (no updatedAt available)
    visits = fetch_recent_visits(token)
    print(f"[POLL] {len(visits)} visits to process", flush=True)
    for node in visits:
        try:
            process_visit(node, since_iso)
        except Exception as exc:
            print(f"[VISIT ERROR] {node.get('id')}: {exc}", flush=True)

    # Expenses
    try:
        expenses = fetch_recent_expenses(token, since_iso)
        print(f"[POLL] {len(expenses)} expenses to process", flush=True)
        for node in expenses:
            try:
                process_expense(node, since_iso)
            except Exception as exc:
                print(f"[EXPENSE ERROR] {node.get('id')}: {exc}", flush=True)
    except Exception as exc:
        print(f"[POLL EXPENSES ERROR] {exc}", flush=True)

    # Timesheets
    try:
        timesheets = fetch_recent_timesheets(token, since_iso)
        print(f"[POLL] {len(timesheets)} timesheets to process", flush=True)
        for node in timesheets:
            try:
                process_timesheet(node, since_iso)
            except Exception as exc:
                print(f"[TIMESHEET ERROR] {node.get('id')}: {exc}", flush=True)
    except Exception as exc:
        print(f"[POLL TIMESHEETS ERROR] {exc}", flush=True)

    _last_error = ""
    _last_poll_time = datetime.now(CENTRAL_TZ).strftime("%b %-d, %Y %-I:%M %p CT")
    print(f"[POLL] Cycle complete at {_last_poll_time}", flush=True)


def polling_loop() -> None:
    """Background thread: initial retroactive load then continuous polling."""
    startup_since = _midnight_central_utc()
    print(f"[POLL] Initial retroactive load since {startup_since.isoformat()}", flush=True)
    run_poll_cycle(startup_since)

    while True:
        time.sleep(POLL_INTERVAL)
        # Subsequent polls: look back 6 minutes to overlap with poll interval
        since = datetime.now(timezone.utc) - timedelta(minutes=6)
        run_poll_cycle(since)


# ── SECTION 6: HTML TEMPLATE ────────────────────────────────────────────────────

_jinja_env = Environment(autoescape=select_autoescape(["html"]))

_BADGE_COLORS = {
    "job":       ("#1a6b3c", "#d4edda"),
    "quote":     ("#0d3b82", "#d0e4ff"),
    "invoice":   ("#7d4e00", "#fff3cd"),
    "visit":     ("#5b2d8e", "#ede0ff"),
    "expense":   ("#7a1c1c", "#fde8e8"),
    "timesheet": ("#1c4e7a", "#e0f0ff"),
}

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="refresh" content="60">
  <title>Jobber Activity Log</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f0f2f5;
      color: #1a1a2e;
    }
    .toolbar {
      background: #1a1a2e;
      color: white;
      padding: 14px 24px;
      display: flex;
      align-items: center;
      gap: 16px;
      flex-wrap: wrap;
    }
    .toolbar h1 { font-size: 1rem; font-weight: 700; flex: 1; }
    .toolbar .meta { font-size: 0.75rem; color: #aaa; }
    .toolbar a.btn, .toolbar button.btn {
      background: #2ecc71;
      color: #111;
      border: none;
      border-radius: 6px;
      padding: 7px 16px;
      font-size: 0.85rem;
      font-weight: 700;
      cursor: pointer;
      text-decoration: none;
      display: inline-block;
    }
    .toolbar a.btn:hover, .toolbar button.btn:hover { background: #27ae60; }
    .toolbar a.btn.secondary { background: #3d3d5c; color: #eee; }
    .toolbar a.btn.secondary:hover { background: #555577; }
    .filters {
      background: white;
      border-bottom: 1px solid #e0e0e0;
      padding: 10px 24px;
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      align-items: center;
    }
    .filters label { font-size: 0.8rem; color: #555; font-weight: 600; }
    .filters select, .filters input {
      padding: 6px 10px;
      border: 1px solid #d0d0d0;
      border-radius: 6px;
      font-size: 0.85rem;
      outline: none;
    }
    .filters select:focus, .filters input:focus { border-color: #888; }
    .count-badge {
      background: #eee;
      border-radius: 12px;
      padding: 2px 8px;
      font-size: 0.75rem;
      color: #555;
      margin-left: auto;
    }
    .error-box {
      background: #fff3f3;
      border: 1px solid #f5c6c6;
      border-radius: 8px;
      padding: 12px 20px;
      margin: 16px 24px;
      color: #c62828;
      font-size: 0.875rem;
    }
    .table-wrap {
      overflow-x: auto;
      padding: 16px 24px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      background: white;
      border-radius: 10px;
      overflow: hidden;
      box-shadow: 0 1px 6px rgba(0,0,0,0.07);
      font-size: 0.85rem;
    }
    thead tr { background: #1a1a2e; color: white; }
    th {
      padding: 11px 14px;
      text-align: left;
      font-weight: 600;
      font-size: 0.78rem;
      letter-spacing: 0.04em;
      white-space: nowrap;
    }
    td {
      padding: 10px 14px;
      border-bottom: 1px solid #f0f0f0;
      vertical-align: top;
    }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: #fafafa; }
    .type-badge {
      display: inline-block;
      border-radius: 4px;
      padding: 2px 8px;
      font-size: 0.72rem;
      font-weight: 700;
      white-space: nowrap;
    }
    .action-cell { color: #333; font-weight: 600; }
    .detail-cell { color: #555; max-width: 300px; }
    .client-cell { color: #333; white-space: nowrap; }
    .employee-cell { color: #555; white-space: nowrap; }
    .time-cell { white-space: nowrap; color: #444; font-size: 0.8rem; }
    .change-pill {
      display: inline-block;
      background: #f5f5f5;
      border-radius: 4px;
      padding: 1px 6px;
      font-size: 0.72rem;
      color: #555;
      margin-top: 3px;
    }
    .empty { text-align: center; padding: 48px; color: #888; }
  </style>
</head>
<body>
  <div class="toolbar">
    <h1>Jobber Activity Log</h1>
    <span class="meta">Updated: {{ generated_at }} &nbsp;|&nbsp; Auto-refreshes every 60s{% if last_error %} &nbsp;|&nbsp; <span style="color:#f66">Poll error</span>{% endif %}</span>
    <a class="btn secondary" href="/login">Lock</a>
    <a class="btn" href="/export.csv">Export CSV</a>
  </div>

  <div class="filters">
    <label>Type:</label>
    <select id="filter-type" onchange="applyFilters()">
      <option value="">All Types</option>
      <option value="job">Job</option>
      <option value="quote">Quote</option>
      <option value="invoice">Invoice</option>
      <option value="visit">Visit</option>
      <option value="expense">Expense</option>
      <option value="timesheet">Timesheet</option>
    </select>
    <label>Action:</label>
    <select id="filter-action" onchange="applyFilters()">
      <option value="">All Actions</option>
      <option value="Created">Created</option>
      <option value="Status Changed">Status Changed</option>
      <option value="Visit Rescheduled">Visit Rescheduled</option>
      <option value="Completed">Completed</option>
      <option value="Team Assignment Changed">Team Assignment Changed</option>
      <option value="Note Added">Note Added</option>
      <option value="Note Edited">Note Edited</option>
      <option value="Expense Entered">Expense Entered</option>
      <option value="Expense Paid">Expense Paid</option>
      <option value="Time Logged">Time Logged</option>
      <option value="Timesheet Approved">Timesheet Approved</option>
      <option value="Updated">Updated</option>
    </select>
    <label>Action By:</label>
    <input type="text" id="filter-actionby" placeholder="User name…" oninput="applyFilters()" style="width:140px">
    <label>Search:</label>
    <input type="text" id="filter-search" placeholder="Client, employee, detail…" oninput="applyFilters()" style="width:200px">
    <span class="count-badge" id="row-count">{{ events|length }} events</span>
  </div>

  {% if last_error %}
  <div class="error-box">Poll error: {{ last_error }}</div>
  {% endif %}

  <div class="table-wrap">
    {% if events %}
    <table id="log-table">
      <thead>
        <tr>
          <th>Time (CT)</th>
          <th>Type</th>
          <th>Reference</th>
          <th>Action</th>
          <th>Client</th>
          <th>Action By</th>
          <th>Employee</th>
          <th>Detail</th>
        </tr>
      </thead>
      <tbody>
        {% for e in events %}
        <tr
          data-type="{{ e.entity_type }}"
          data-action="{{ e.action }}"
          data-actionby="{{ (e.action_by or '')|lower }}"
          data-search="{{ (e.client_name ~ ' ' ~ e.employee ~ ' ' ~ (e.action_by or '') ~ ' ' ~ e.detail ~ ' ' ~ e.entity_ref)|lower }}"
        >
          <td class="time-cell">{{ fmt_dt(e.event_time) }}</td>
          <td>
            {% set bc = badge_color(e.entity_type) %}
            <span class="type-badge" style="color:{{ bc[0] }};background:{{ bc[1] }}">{{ e.entity_type|upper }}</span>
          </td>
          <td style="white-space:nowrap;font-weight:600">{{ e.entity_ref }}</td>
          <td class="action-cell">{{ e.action }}</td>
          <td class="client-cell">{{ e.client_name }}</td>
          <td class="employee-cell" style="color:#1a6b3c;font-weight:{% if e.action_by %}600{% else %}400{% endif %}">{{ e.action_by or '' }}</td>
          <td class="employee-cell">{{ e.employee }}</td>
          <td class="detail-cell">
            {{ e.detail }}
          </td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
    {% else %}
    <div class="empty">No activity recorded yet. Polling every 2 minutes.</div>
    {% endif %}
  </div>

  <script>
    function applyFilters() {
      const type = document.getElementById('filter-type').value.toLowerCase();
      const action = document.getElementById('filter-action').value.toLowerCase();
      const actionby = document.getElementById('filter-actionby').value.toLowerCase();
      const search = document.getElementById('filter-search').value.toLowerCase();
      const rows = document.querySelectorAll('#log-table tbody tr');
      let visible = 0;
      rows.forEach(row => {
        const matchType = !type || row.dataset.type === type;
        const matchAction = !action || row.dataset.action.toLowerCase().includes(action);
        const matchActionBy = !actionby || (row.dataset.actionby || '').includes(actionby);
        const matchSearch = !search || row.dataset.search.includes(search);
        const show = matchType && matchAction && matchActionBy && matchSearch;
        row.style.display = show ? '' : 'none';
        if (show) visible++;
      });
      document.getElementById('row-count').textContent = visible + ' events';
    }
  </script>
</body>
</html>"""


_LOGIN_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Activity Log</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #f0f2f5;
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 100vh;
    }
    .card {
      background: white;
      border-radius: 12px;
      padding: 36px 32px;
      box-shadow: 0 2px 12px rgba(0,0,0,0.1);
      width: 280px;
      text-align: center;
    }
    h1 { font-size: 1.1rem; margin-bottom: 24px; color: #1a1a2e; }
    input[type="password"] {
      width: 100%;
      padding: 10px 14px;
      font-size: 1.4rem;
      letter-spacing: 0.3em;
      text-align: center;
      border: 1px solid #d0d0d0;
      border-radius: 8px;
      outline: none;
      margin-bottom: 14px;
    }
    input[type="password"]:focus { border-color: #888; }
    button {
      width: 100%;
      padding: 10px;
      font-size: 0.95rem;
      font-weight: 600;
      background: #1a1a2e;
      color: white;
      border: none;
      border-radius: 8px;
      cursor: pointer;
    }
    button:hover { background: #2d2d4e; }
    .error { color: #c62828; font-size: 0.85rem; margin-top: 12px; }
  </style>
</head>
<body>
  <div class="card">
    <h1>Jobber Activity Log</h1>
    <form method="post" action="/login">
      <input type="password" name="pin" placeholder="PIN" autofocus maxlength="20">
      <button type="submit">Enter</button>
      {% if error %}<p class="error">Incorrect PIN</p>{% endif %}
    </form>
  </div>
</body>
</html>"""


# ── SECTION 7: AUTHENTICATION ────────────────────────────────────────────────────

_SESSION_COOKIE = "actlog_session"


def _session_token() -> str:
    pin = os.environ.get("DASHBOARD_PIN", "")
    return hmac.new(pin.encode(), b"actlog_session", hashlib.sha256).hexdigest()


def _is_authenticated(request: Request) -> bool:
    cookie = request.cookies.get(_SESSION_COOKIE, "")
    expected = _session_token()
    return bool(expected) and secrets.compare_digest(cookie, expected)


# ── SECTION 8: FASTAPI APP ────────────────────────────────────────────────────────

app = FastAPI(title="Jobber Activity Log")


@app.on_event("startup")
def startup_event() -> None:
    init_db()
    t = threading.Thread(target=polling_loop, daemon=True)
    t.start()


@app.get("/login", response_class=HTMLResponse)
async def login_page(error: str = ""):
    tmpl = _jinja_env.from_string(_LOGIN_TEMPLATE)
    return HTMLResponse(tmpl.render(error=error))


@app.post("/login")
async def login_submit(pin: str = Form(...)):
    expected = os.environ.get("DASHBOARD_PIN", "")
    if expected and secrets.compare_digest(pin, expected):
        response = RedirectResponse(url="/", status_code=303)
        response.set_cookie(_SESSION_COOKIE, _session_token(), httponly=True, samesite="lax")
        return response
    return RedirectResponse(url="/login?error=1", status_code=303)


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    if not _is_authenticated(request):
        return RedirectResponse(url="/login", status_code=303)

    events = get_events(limit=2000)

    now_ct = datetime.now(CENTRAL_TZ)
    hour = now_ct.hour % 12 or 12
    generated_at = (
        f"{now_ct.strftime('%b')} {now_ct.day}, {now_ct.year} "
        f"{hour}:{now_ct.strftime('%M')} "
        f"{'AM' if now_ct.hour < 12 else 'PM'} CT"
    )

    def fmt_dt(iso: str) -> str:
        return _fmt_dt_central(iso)

    def badge_color(entity_type: str) -> tuple:
        return _BADGE_COLORS.get(entity_type, ("#333", "#eee"))

    tmpl = _jinja_env.from_string(_HTML_TEMPLATE)
    html = tmpl.render(
        events=events,
        generated_at=generated_at,
        last_error=_last_error,
        fmt_dt=fmt_dt,
        badge_color=badge_color,
    )
    return HTMLResponse(content=html)


@app.get("/export.csv")
async def export_csv(request: Request):
    if not _is_authenticated(request):
        return RedirectResponse(url="/login", status_code=303)

    rows = get_all_events_for_csv()
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[
        "event_time", "entity_type", "entity_ref", "action",
        "client_name", "action_by", "employee", "detail", "old_value", "new_value", "logged_at",
    ])
    writer.writeheader()
    for row in rows:
        writer.writerow(row)

    filename = f"jobber_activity_{datetime.now(CENTRAL_TZ).strftime('%Y%m%d_%H%M')}.csv"
    return PlainTextResponse(
        content=output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── SECTION 9: WEBHOOK ENDPOINT ───────────────────────────────────────────────────
#
# Configure Jobber to POST webhooks to: https://<your-domain>/webhook/jobber
#
# In Jobber Developer Console → App Settings → Webhooks, add the URL and copy
# the webhook secret into Railway env var JOBBER_WEBHOOK_SECRET.
#
# Supported Jobber webhook topics (subscribe to these):
#   JOB_CREATE, JOB_UPDATE, JOB_DELETE
#   QUOTE_CREATE, QUOTE_UPDATE, QUOTE_DELETE
#   INVOICE_CREATE, INVOICE_UPDATE, INVOICE_DELETE
#   VISIT_CREATE, VISIT_UPDATE, VISIT_DELETE
#   EXPENSE_CREATE, EXPENSE_UPDATE
#   TIMESHEET_ENTRY_CREATE, TIMESHEET_ENTRY_UPDATE

def _verify_jobber_webhook(body: bytes, signature_header: str) -> bool:
    """Verify Jobber's HMAC-SHA256 webhook signature.

    Jobber sends X-Jobber-Hmac-SHA256: base64(HMAC-SHA256(secret, body)).
    Returns True if the signature matches or if no secret is configured (dev mode).
    """
    secret = os.environ.get("JOBBER_WEBHOOK_SECRET", "")
    if not secret:
        # No secret configured — accept all (dev/testing mode only)
        return True
    import base64
    expected = base64.b64encode(
        hmac.new(secret.encode(), body, hashlib.sha256).digest()
    ).decode()
    return secrets.compare_digest(signature_header or "", expected)


def _handle_webhook_payload(topic: str, object_id: str) -> None:
    """Background task: re-fetch the affected object and run change detection."""
    print(f"[WEBHOOK] topic={topic} id={object_id}", flush=True)
    try:
        token = refresh_access_token()
    except Exception as exc:
        print(f"[WEBHOOK] token refresh failed: {exc}", flush=True)
        return

    since_iso = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    topic_upper = topic.upper()

    if topic_upper.startswith("JOB_"):
        node = fetch_single_job(token, object_id)
        if node:
            try:
                process_job(node, since_iso)
            except Exception as exc:
                print(f"[WEBHOOK] process_job error: {exc}", flush=True)

    elif topic_upper.startswith("QUOTE_"):
        node = fetch_single_quote(token, object_id)
        if node:
            try:
                process_quote(node, since_iso)
            except Exception as exc:
                print(f"[WEBHOOK] process_quote error: {exc}", flush=True)

    elif topic_upper.startswith("INVOICE_"):
        node = fetch_single_invoice(token, object_id)
        if node:
            try:
                process_invoice(node, since_iso)
            except Exception as exc:
                print(f"[WEBHOOK] process_invoice error: {exc}", flush=True)

    elif topic_upper.startswith("VISIT_"):
        node = fetch_single_visit(token, object_id)
        if node:
            try:
                process_visit(node, since_iso)
            except Exception as exc:
                print(f"[WEBHOOK] process_visit error: {exc}", flush=True)

    else:
        print(f"[WEBHOOK] unhandled topic: {topic}", flush=True)


@app.post("/webhook/jobber")
async def jobber_webhook(request: Request, background_tasks: BackgroundTasks):
    """Receive real-time event notifications from Jobber.

    Jobber calls this endpoint whenever a subscribed object changes.
    We verify the signature, extract the topic + object ID, then re-fetch
    the object in the background so change detection runs immediately
    (rather than waiting for the next 2-minute poll cycle).
    """
    body = await request.body()
    signature = request.headers.get("X-Jobber-Hmac-SHA256", "")

    if not _verify_jobber_webhook(body, signature):
        print("[WEBHOOK] Invalid signature — rejected", flush=True)
        return JSONResponse({"error": "invalid signature"}, status_code=401)

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return JSONResponse({"error": "bad json"}, status_code=400)

    # Jobber webhook payload shape: { "webHookEvent": { "topic": "...", "appId": "...",
    #   "accountId": "...", "data": { "item": { "id": "..." } } } }
    event = payload.get("webHookEvent", payload)
    topic = event.get("topic", "")
    object_id = (event.get("data", {}).get("item") or {}).get("id", "")

    if not topic or not object_id:
        print(f"[WEBHOOK] Missing topic or id in payload: {payload}", flush=True)
        return JSONResponse({"ok": True})  # Still 200 so Jobber doesn't retry

    background_tasks.add_task(_handle_webhook_payload, topic, object_id)
    return JSONResponse({"ok": True})


@app.get("/status")
async def status():
    """Health check endpoint — returns last poll time and any error."""
    return JSONResponse({
        "last_poll": _last_poll_time,
        "last_error": _last_error,
        "webhook_secret_configured": bool(os.environ.get("JOBBER_WEBHOOK_SECRET")),
    })
