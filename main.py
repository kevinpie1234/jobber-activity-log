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
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
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


def insert_event(
    event_time: str,
    entity_type: str,
    entity_id: str,
    entity_ref: str,
    action: str,
    detail: str = "",
    client_name: str = "",
    employee: str = "",
    old_value: str = "",
    new_value: str = "",
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _db_lock, sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """INSERT INTO activity_log
               (logged_at, event_time, entity_type, entity_id, entity_ref,
                action, detail, client_name, employee, old_value, new_value)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (now, event_time, entity_type, entity_id, entity_ref,
             action, detail, client_name, employee, old_value, new_value),
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
                      client_name, employee, old_value, new_value, logged_at
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
      client {
        firstName
        lastName
        companyName
        isCompany
      }
      assignedUsers(first: 10) {
        nodes {
          id
          name { full }
        }
      }
      notes(first: 20) {
        nodes {
          id
          title
          content
          createdAt
          author { name { full } }
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
      total
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
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

VISITS_QUERY = """
query VisitsActivity($filter: VisitFilterAttributes!, $first: Int!) {
  visits(filter: $filter, first: $first) {
    nodes {
      id
      startAt
      endAt
      allDay
      createdAt
      updatedAt
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
    """Page through a Jobber connection until items are older than since_iso.

    Assumes Jobber returns items newest-first (default sort). Stops early when
    the last node on a page is older than since_iso.
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

        last_time = page_nodes[-1].get("updatedAt") or page_nodes[-1].get("createdAt") or ""
        if last_time < since_iso or not page_info.get("hasNextPage"):
            break

        after = page_info.get("endCursor")

    return nodes


def fetch_recent_jobs(access_token: str, since_iso: str) -> list[dict]:
    return _fetch_paginated(access_token, JOBS_QUERY, "jobs", since_iso)


def fetch_recent_quotes(access_token: str, since_iso: str) -> list[dict]:
    return _fetch_paginated(access_token, QUOTES_QUERY, "quotes", since_iso)


def fetch_recent_invoices(access_token: str, since_iso: str) -> list[dict]:
    return _fetch_paginated(access_token, INVOICES_QUERY, "invoices", since_iso)


def fetch_recent_visits(access_token: str, since_utc: datetime) -> list[dict]:
    """Fetch visits whose startAt falls within a broad window around now."""
    # Cover visits from yesterday through 60 days out to catch reschedules
    window_start = (since_utc - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    window_end = (datetime.now(timezone.utc) + timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    since_iso = since_utc.isoformat()

    try:
        data = _run_query(
            access_token,
            VISITS_QUERY,
            {"filter": {"startAt": {"after": window_start, "before": window_end}}, "first": 200},
        )
        nodes = data["visits"]["nodes"]
    except Exception as exc:
        print(f"[VISITS QUERY ERROR] {exc}", flush=True)
        return []

    return [n for n in nodes if (n.get("updatedAt") or n.get("createdAt") or "") >= since_iso]


# ── SECTION 4: CHANGE DETECTION ─────────────────────────────────────────────────

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
    """Detect changes on a job node and insert activity events."""
    eid = node["id"]
    ref = f"Job #{node.get('jobNumber', '?')}"
    client = _client_name(node.get("client"))
    users = _assigned_users(node)
    employee = ", ".join(users) if users else "Unassigned"
    event_time = node.get("updatedAt") or node.get("createdAt") or ""
    created_at = node.get("createdAt") or ""

    stored = get_entity_state("job", eid)

    # Snapshot of current state for storage
    note_ids = [n["id"] for n in (node.get("notes") or {}).get("nodes", [])]
    current_state = {
        "jobStatus": node.get("jobStatus", ""),
        "title": node.get("title", ""),
        "assignedUsers": sorted(users),
        "noteIds": sorted(note_ids),
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
                detail=f"Status: {_fmt_status(node.get('jobStatus', ''))}",
                client_name=client,
                employee=employee,
            )
        upsert_entity_state("job", eid, current_state, event_time)
        return

    prev = stored["state"]
    prev_updated = stored["updated_at"]

    # Skip if nothing changed
    if event_time and prev_updated and event_time <= prev_updated:
        return

    # Compare fields
    if prev.get("jobStatus") != current_state["jobStatus"]:
        insert_event(
            event_time=event_time,
            entity_type="job",
            entity_id=eid,
            entity_ref=ref,
            action="Status Changed",
            detail=f"{_fmt_status(prev.get('jobStatus', ''))} → {_fmt_status(current_state['jobStatus'])}",
            client_name=client,
            employee=employee,
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
            employee=employee,
            old_value=prev.get("title", ""),
            new_value=current_state["title"],
        )

    if prev.get("assignedUsers", []) != current_state["assignedUsers"]:
        old_users = ", ".join(prev.get("assignedUsers", [])) or "Unassigned"
        new_users = ", ".join(current_state["assignedUsers"]) or "Unassigned"
        insert_event(
            event_time=event_time,
            entity_type="job",
            entity_id=eid,
            entity_ref=ref,
            action="Team Assignment Changed",
            detail=f"{old_users} → {new_users}",
            client_name=client,
            employee=employee,
            old_value=old_users,
            new_value=new_users,
        )

    # Check for new notes
    prev_note_ids = set(prev.get("noteIds", []))
    for note_node in (node.get("notes") or {}).get("nodes", []):
        if note_node["id"] not in prev_note_ids:
            author = ""
            try:
                author = note_node["author"]["name"]["full"]
            except (KeyError, TypeError):
                pass
            note_title = note_node.get("title") or ""
            note_content = note_node.get("content") or ""
            note_preview = (note_title or note_content)[:120]
            insert_event(
                event_time=note_node.get("createdAt") or event_time,
                entity_type="job",
                entity_id=eid,
                entity_ref=ref,
                action="Note Added",
                detail=note_preview,
                client_name=client,
                employee=author or employee,
            )

    # If updatedAt changed but no specific field change detected, log generic update
    if (
        event_time != prev_updated
        and prev.get("jobStatus") == current_state["jobStatus"]
        and prev.get("title") == current_state["title"]
        and prev.get("assignedUsers", []) == current_state["assignedUsers"]
        and set(prev.get("noteIds", [])) == set(note_ids)
    ):
        insert_event(
            event_time=event_time,
            entity_type="job",
            entity_id=eid,
            entity_ref=ref,
            action="Updated",
            detail="Details edited",
            client_name=client,
            employee=employee,
        )

    upsert_entity_state("job", eid, current_state, event_time)


def process_quote(node: dict, since_iso: str) -> None:
    eid = node["id"]
    ref = f"Quote #{node.get('quoteNumber', '?')}"
    client = _client_name(node.get("client"))
    event_time = node.get("updatedAt") or node.get("createdAt") or ""
    created_at = node.get("createdAt") or ""

    stored = get_entity_state("quote", eid)

    current_state = {
        "quoteStatus": node.get("quoteStatus", ""),
        "title": node.get("title", ""),
        "total": str(node.get("total", "")),
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
                detail=f"Status: {_fmt_status(node.get('quoteStatus', ''))} | Total: {_fmt_currency(node.get('total'))}",
                client_name=client,
            )
        upsert_entity_state("quote", eid, current_state, event_time)
        return

    prev = stored["state"]
    prev_updated = stored["updated_at"]

    if event_time and prev_updated and event_time <= prev_updated:
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
            detail=f"{_fmt_currency(prev.get('total'))} → {_fmt_currency(node.get('total'))}",
            client_name=client,
            old_value=_fmt_currency(prev.get("total")),
            new_value=_fmt_currency(node.get("total")),
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
        upsert_entity_state("invoice", eid, current_state, event_time)
        return

    prev = stored["state"]
    prev_updated = stored["updated_at"]

    if event_time and prev_updated and event_time <= prev_updated:
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

    upsert_entity_state("invoice", eid, current_state, event_time)


def process_visit(node: dict, since_iso: str) -> None:
    eid = node["id"]
    job = node.get("job") or {}
    job_num = job.get("jobNumber", "?")
    ref = f"Visit (Job #{job_num})"
    client = _client_name(node.get("client"))
    users = _assigned_users(node)
    employee = ", ".join(users) if users else "Unassigned"
    event_time = node.get("updatedAt") or node.get("startAt") or ""
    created_at = node.get("createdAt") or ""

    stored = get_entity_state("visit", eid)

    current_state = {
        "startAt": node.get("startAt", ""),
        "endAt": node.get("endAt", ""),
        "allDay": node.get("allDay", False),
        "assignedUsers": sorted(users),
        "updatedAt": event_time,
    }

    if stored is None:
        if created_at >= since_iso or (node.get("startAt") or "") >= since_iso:
            start_fmt = _fmt_dt_central(node.get("startAt", ""))
            insert_event(
                event_time=created_at or node.get("startAt", ""),
                entity_type="visit",
                entity_id=eid,
                entity_ref=ref,
                action="Created",
                detail=f"Scheduled: {start_fmt}",
                client_name=client,
                employee=employee,
            )
        upsert_entity_state("visit", eid, current_state, event_time)
        return

    prev = stored["state"]
    prev_updated = stored["updated_at"]

    if event_time and prev_updated and event_time <= prev_updated:
        return

    if prev.get("startAt") != current_state["startAt"]:
        old_fmt = _fmt_dt_central(prev.get("startAt", ""))
        new_fmt = _fmt_dt_central(current_state["startAt"])
        insert_event(
            event_time=event_time,
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
    elif prev.get("endAt") != current_state["endAt"]:
        old_fmt = _fmt_dt_central(prev.get("endAt", ""))
        new_fmt = _fmt_dt_central(current_state["endAt"])
        insert_event(
            event_time=event_time,
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
            event_time=event_time,
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

    if (
        event_time != prev_updated
        and prev.get("startAt") == current_state["startAt"]
        and prev.get("endAt") == current_state["endAt"]
        and prev.get("assignedUsers", []) == current_state["assignedUsers"]
    ):
        insert_event(
            event_time=event_time,
            entity_type="visit",
            entity_id=eid,
            entity_ref=ref,
            action="Updated",
            detail="Details edited",
            client_name=client,
            employee=employee,
        )

    upsert_entity_state("visit", eid, current_state, event_time)


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
    try:
        visits = fetch_recent_visits(token, since_utc)
        print(f"[POLL] {len(visits)} visits to process", flush=True)
        for node in visits:
            try:
                process_visit(node, since_iso)
            except Exception as exc:
                print(f"[VISIT ERROR] {node.get('id')}: {exc}", flush=True)
    except Exception as exc:
        print(f"[POLL VISITS ERROR] {exc}", flush=True)

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
    "job":     ("#1a6b3c", "#d4edda"),
    "quote":   ("#0d3b82", "#d0e4ff"),
    "invoice": ("#7d4e00", "#fff3cd"),
    "visit":   ("#5b2d8e", "#ede0ff"),
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
      <option value="">All</option>
      <option value="job">Job</option>
      <option value="quote">Quote</option>
      <option value="invoice">Invoice</option>
      <option value="visit">Visit</option>
    </select>
    <label>Action:</label>
    <select id="filter-action" onchange="applyFilters()">
      <option value="">All</option>
      <option value="Created">Created</option>
      <option value="Status Changed">Status Changed</option>
      <option value="Visit Rescheduled">Visit Rescheduled</option>
      <option value="Team Assignment Changed">Team Assignment Changed</option>
      <option value="Note Added">Note Added</option>
      <option value="Updated">Updated</option>
    </select>
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
          <th>Employee</th>
          <th>Detail</th>
        </tr>
      </thead>
      <tbody>
        {% for e in events %}
        <tr
          data-type="{{ e.entity_type }}"
          data-action="{{ e.action }}"
          data-search="{{ (e.client_name ~ ' ' ~ e.employee ~ ' ' ~ e.detail ~ ' ' ~ e.entity_ref)|lower }}"
        >
          <td class="time-cell">{{ fmt_dt(e.event_time) }}</td>
          <td>
            {% set bc = badge_color(e.entity_type) %}
            <span class="type-badge" style="color:{{ bc[0] }};background:{{ bc[1] }}">{{ e.entity_type|upper }}</span>
          </td>
          <td style="white-space:nowrap;font-weight:600">{{ e.entity_ref }}</td>
          <td class="action-cell">{{ e.action }}</td>
          <td class="client-cell">{{ e.client_name }}</td>
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
      const search = document.getElementById('filter-search').value.toLowerCase();
      const rows = document.querySelectorAll('#log-table tbody tr');
      let visible = 0;
      rows.forEach(row => {
        const matchType = !type || row.dataset.type === type;
        const matchAction = !action || row.dataset.action.toLowerCase().includes(action);
        const matchSearch = !search || row.dataset.search.includes(search);
        const show = matchType && matchAction && matchSearch;
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
        "client_name", "employee", "detail", "old_value", "new_value", "logged_at",
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
