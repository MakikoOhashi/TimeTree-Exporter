#!/usr/bin/env python3
"""Small, serial, restartable TimeTree -> Notion batch.

Real runs require either --limit 1..3 or the explicit --all flag.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

API = "https://api.notion.com/v1"
DATA_SOURCE = "3c2c3eba-44cb-800c-a7f1-000b9bb28d32"
STATE_DIR = Path("/workspace/dev/.timetree-notion-state")
RETRYABLE = {500, 502, 503, 504, 529}


class AuthFailure(RuntimeError):
    pass


def safe_error(response: requests.Response) -> tuple[str, str]:
    try:
        body = response.json()
        code = str(body.get("code", "http_error"))
        message = str(body.get("message", ""))[:240]
    except ValueError:
        code, message = "http_error", ""
    return code, message.replace("Bearer ", "Bearer [redacted]")


class Client:
    def __init__(self, token: str) -> None:
        self.session = requests.Session()
        self.headers = {"Authorization": f"Bearer {token}", "Notion-Version": "2026-03-11"}
        self.last_request = 0.0

    def request(self, method: str, url: str, *, retry: bool = True, **kwargs: Any) -> requests.Response:
        attempts, delay = 0, 2.0
        while True:
            wait = 2.0 - (time.monotonic() - self.last_request)
            if wait > 0:
                time.sleep(wait)
            self.last_request = time.monotonic()
            try:
                response = self.session.request(method, url, headers=self.headers, timeout=60, **kwargs)
            except requests.RequestException:
                if not retry or attempts >= 2:
                    raise
                attempts += 1
                time.sleep(delay + random.uniform(0, 0.5))
                delay *= 2
                continue
            if response.status_code in (401, 403):
                code, message = safe_error(response)
                raise AuthFailure(f"{response.status_code} {code}: {message}")
            if response.status_code in (429, 529):
                if not retry or attempts >= 2:
                    return response
                attempts += 1
                try:
                    wait_seconds = max(float(response.headers.get("Retry-After", "0")), delay)
                except ValueError:
                    wait_seconds = delay
                time.sleep(wait_seconds + random.uniform(0, 0.5))
                delay *= 2
                continue
            if response.status_code in RETRYABLE:
                if not retry or attempts >= 2:
                    return response
                attempts += 1
                time.sleep(delay + random.uniform(0, 0.5))
                delay *= 2
                continue
            return response

    def existing_pages(self) -> list[dict[str, Any]]:
        result, cursor = [], None
        while True:
            payload = {"page_size": 100}
            if cursor:
                payload["start_cursor"] = cursor
            response = self.request("POST", f"{API}/data_sources/{DATA_SOURCE}/query", json=payload)
            if response.status_code >= 300:
                code, _ = safe_error(response)
                raise RuntimeError(f"database_query {response.status_code} {code}")
            data = response.json()
            result.extend(data.get("results", []))
            if not data.get("has_more"):
                return result
            cursor = data.get("next_cursor")

    def upload(self, path: Path) -> str:
        response = self.request(
            "POST", f"{API}/file_uploads",
            json={"mode": "single_part", "filename": path.name, "content_type": "image/jpeg"},
        )
        if response.status_code >= 300:
            code, _ = safe_error(response)
            raise RuntimeError(f"upload_create {response.status_code} {code}")
        upload = response.json()
        with path.open("rb") as image:
            response = self.request(
                "POST", upload["upload_url"],
                files={"file": (path.name, image, "image/jpeg")},
            )
        if response.status_code >= 300 or response.json().get("status") != "uploaded":
            code, _ = safe_error(response)
            raise RuntimeError(f"upload_send {response.status_code} {code}")
        return str(response.json()["id"])

    def create_page(self, event: dict[str, Any], uploads: list[tuple[str, Path]]) -> str:
        properties = {
            "Name": {"title": [{"type": "text", "text": {"content": event["title"]}}]},
            "Date": {"date": {"start": event["date"]}},
            "TimeTree UUID": {"rich_text": [{"type": "text", "text": {"content": event["uuid"]}}]},
            "TimeTree Image": {"files": [
                {"type": "file_upload", "file_upload": {"id": uid}, "name": path.name}
                for uid, path in uploads
            ]},
        }
        response = self.request(
            "POST", f"{API}/pages", retry=False,
            json={"parent": {"type": "data_source_id", "data_source_id": DATA_SOURCE}, "properties": properties},
        )
        if response.status_code >= 300:
            code, _ = safe_error(response)
            raise RuntimeError(f"page_create {response.status_code} {code}")
        return str(response.json()["id"])


def token_from_env() -> str:
    for line in (Path(__file__).with_name(".env")).read_text(encoding="utf-8").splitlines():
        if line.startswith("NOTION_TOKEN=") and line.split("=", 1)[1].strip():
            return line.split("=", 1)[1].strip()
    raise RuntimeError("NOTION_TOKEN is missing")


def events_from_manifest(root: Path) -> list[dict[str, Any]]:
    manifest = json.loads((root / "timetree_images.json").read_text(encoding="utf-8"))
    events: dict[str, dict[str, Any]] = {}
    for item in manifest:
        event = events.setdefault(item["event_uuid"], {
            "uuid": item["event_uuid"], "title": item["title"], "date": item["start_date"], "images": []
        })
        event["images"].append(root / item["image_path"])
    return sorted(events.values(), key=lambda e: (e["date"], e["uuid"]))


def _ics_value(value: str) -> str:
    return value.replace("\\n", "\n").replace("\\,", ",").replace("\\;", ";").replace("\\\\", "\\")


def _date_value(value: str) -> str:
    compact = value[:8]
    if len(compact) == 8 and compact.isdigit():
        return f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"
    return value[:10]


def events_from_sources(root: Path) -> list[dict[str, Any]]:
    """Build all ICS events and attach any local images from the manifest."""
    manifest_events = {event["uuid"]: event for event in events_from_manifest(root)}
    lines = (root / "timetree.ics").read_text(encoding="utf-8").splitlines()
    unfolded: list[str] = []
    for line in lines:
        if line.startswith((" ", "\t")) and unfolded:
            unfolded[-1] += line[1:]
        else:
            unfolded.append(line)
    events: dict[str, dict[str, Any]] = {}
    in_event = False
    fields: dict[str, str] = {}
    for line in unfolded + ["END:VEVENT"]:
        if line == "BEGIN:VEVENT":
            in_event, fields = True, {}
            continue
        if line != "END:VEVENT" or not in_event:
            if in_event and ":" in line:
                key, value = line.split(":", 1)
                fields[key.split(";", 1)[0]] = _ics_value(value)
            continue
        uuid, title, start = fields.get("UID"), fields.get("SUMMARY", ""), fields.get("DTSTART", "")
        if uuid:
            events[uuid] = {"uuid": uuid, "title": title, "date": _date_value(start), "images": []}
        in_event = False
    for uuid, event in manifest_events.items():
        events.setdefault(uuid, event)
        events[uuid]["images"] = event["images"]
        if not events[uuid]["title"]:
            events[uuid]["title"] = event["title"]
        if not events[uuid]["date"]:
            events[uuid]["date"] = event["date"]
    return sorted(events.values(), key=lambda e: (e["date"], e["uuid"]))


def uuid_from_page(page: dict[str, Any]) -> str | None:
    values = page.get("properties", {}).get("TimeTree UUID", {}).get("rich_text", [])
    return values[0].get("plain_text") if values else None


def progress_add(path: Path, uuid: str) -> None:
    values = json.loads(path.read_text(encoding="utf-8")) if path.exists() else []
    if uuid in values:
        return
    values.append(uuid)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(values, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def log_error(path: Path, event: dict[str, Any], stage: str, error: Exception) -> None:
    record = {"time": datetime.now().astimezone().isoformat(timespec="seconds"), "uuid": event["uuid"],
              "title": event["title"], "stage": stage, "error": str(error)[:300]}
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, ensure_ascii=False) + "\n")
    os.chmod(path, 0o600)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--all", action="store_true", help="explicitly allow the full migration")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--uuid", action="append", dest="uuids")
    parser.add_argument("--root", type=Path, default=Path(__file__).parent)
    parser.add_argument("--state-dir", type=Path, default=STATE_DIR)
    args = parser.parse_args()
    if not args.dry_run and not args.all and (args.limit is None or not 1 <= args.limit <= 3):
        parser.error("real runs require --limit 1..3 or explicit --all")
    root = args.root.resolve()
    args.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    progress = args.state_dir / "progress.json"
    errors = args.state_dir / "errors.jsonl"
    token = token_from_env()
    client = Client(token)
    pages = client.existing_pages()
    notion_uuids = [uuid for page in pages if (uuid := uuid_from_page(page))]
    existing = set(notion_uuids)
    duplicate_count = 0
    for uuid, count in sorted(Counter(notion_uuids).items()):
        if count > 1:
            duplicate_count += count - 1
            print(f"warning duplicate_notion_uuid {uuid} count={count}")
    done = set(json.loads(progress.read_text(encoding="utf-8"))) if progress.exists() else set()
    known = existing | done
    all_events = events_from_sources(root)
    skipped_existing = sum(e["uuid"] in existing for e in all_events)
    skipped_progress = sum(e["uuid"] in done and e["uuid"] not in existing for e in all_events)
    events = [e for e in all_events if e["uuid"] not in known]
    if args.uuids:
        wanted = set(args.uuids)
        events = [e for e in events if e["uuid"] in wanted]
    if args.limit:
        events = events[: args.limit]
    summary = {
        "manifest_total_events": len(all_events), "selected": len(events),
        "new_pages": 0, "skip_count": skipped_existing + skipped_progress,
        "image_upload_success": 0, "image_upload_failed": 0,
        "warning_count": duplicate_count, "progress_uuid_count": len(done),
        "notion_duplicate_uuid_count": duplicate_count,
    }
    print(
        f"existing_uuid_count={len(existing)} progress_uuid_count={len(done)} "
        f"skipped_existing={skipped_existing} skipped_progress={skipped_progress} selected={len(events)}"
    )
    if args.limit or args.uuids:
        for event in events:
            print(f"candidate {event['uuid']} | {event['date']} | {event['title']} | images={len(event['images'])}")
    elif args.dry_run:
        print("candidate_output_suppressed=true (use --limit N for details)")
    if args.dry_run:
        summary_path = args.state_dir / "last_summary.json"
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.chmod(summary_path, 0o600)
        return 0
    for event in events:
        missing = [path for path in event["images"] if not path.is_file()]
        if missing:
            summary["image_upload_failed"] += len(missing)
            summary["warning_count"] += 1
            log_error(errors, event, "preflight", RuntimeError(f"missing_images={len(missing)}"))
            print(f"failed {event['uuid']} missing_images={len(missing)}")
            continue
        try:
            uploads = []
            for path in event["images"]:
                try:
                    uploads.append((client.upload(path), path))
                    summary["image_upload_success"] += 1
                except Exception:
                    summary["image_upload_failed"] += 1
                    raise
            page_id = client.create_page(event, uploads)
            progress_add(progress, event["uuid"])
            existing.add(event["uuid"])
            summary["new_pages"] += 1
            print(f"success {event['uuid']} page={page_id} images={len(uploads)}")
        except AuthFailure:
            raise
        except Exception as error:
            summary["warning_count"] += 1
            log_error(errors, event, "event", error)
            print(f"failed {event['uuid']} error={type(error).__name__}")
    summary["progress_uuid_count"] = len(json.loads(progress.read_text(encoding="utf-8"))) if progress.exists() else 0
    summary_path = args.state_dir / "last_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.chmod(summary_path, 0o600)
    print("summary " + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AuthFailure:
        print("fatal_auth", file=sys.stderr)
        raise SystemExit(2)
