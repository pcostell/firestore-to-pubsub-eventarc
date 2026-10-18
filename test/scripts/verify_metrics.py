#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Verifies Firestore, Cloud Run, and Pub/Sub metrics and job summaries."""

import argparse
import datetime
import json
import subprocess
import sys
import time
from typing import Any
import urllib.parse
import urllib.request


def get_access_token() -> str:
  for cmd in (
      ["gcloud", "auth", "application-default", "print-access-token"],
      ["gcloud", "auth", "print-access-token"],
  ):
    res = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if res.returncode == 0 and res.stdout.strip():
      return res.stdout.strip()
  raise RuntimeError("Failed to obtain access token from gcloud.")


def query_monitoring_sum(
    project_id: str,
    token: str,
    metric_filter: str,
    start_iso: str,
    end_iso: str,
) -> int:
  """Queries Cloud Monitoring timeSeries API and sums all INT64/DOUBLE points."""
  total = 0
  page_token = ""
  while True:
    query_dict = {
        "filter": metric_filter,
        "interval.startTime": start_iso,
        "interval.endTime": end_iso,
        "pageSize": "1000",
    }
    if page_token:
      query_dict["pageToken"] = page_token
    params = urllib.parse.urlencode(query_dict)
    url = f"https://monitoring.googleapis.com/v3/projects/{project_id}/timeSeries?{params}"
    req = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {token}"}
    )
    try:
      with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:  # pylint: disable=broad-except
      print(f"Warning: monitoring query failed for {metric_filter}: {exc}")
      return total

    for ts in payload.get("timeSeries", []):
      for pt in ts.get("points", []):
        val = pt.get("value", {})
        if "int64Value" in val:
          total += int(val["int64Value"])
        elif "doubleValue" in val:
          total += int(val["doubleValue"])
    page_token = str(payload.get("nextPageToken") or "")
    if not page_token:
      break
  return total


def fetch_job_summaries(
    project_id: str,
    job_name: str,
    marker: str,
    start_iso: str,
    run_id: str = "",
) -> list[dict[str, Any]]:
  """Reads structured summary lines emitted by a Cloud Run v2 Job."""
  log_filter = (
      'resource.type="cloud_run_job" '
      f'AND resource.labels.job_name="{job_name}" '
      f'AND timestamp>="{start_iso}" '
      f'AND textPayload:"{marker}"'
  )
  cmd = [
      "gcloud",
      "logging",
      "read",
      log_filter,
      f"--project={project_id}",
      "--limit=50",
      "--format=json",
  ]
  res = subprocess.run(cmd, capture_output=True, text=True, check=False)
  if res.returncode != 0 or not res.stdout.strip():
    return []
  entries = json.loads(res.stdout)
  summaries: list[dict[str, Any]] = []
  seen_payloads: set[str] = set()
  for entry in entries:
    text = str(entry.get("textPayload") or "")
    if marker in text:
      json_part = text.split(marker, 1)[1].strip()
      if json_part not in seen_payloads:
        seen_payloads.add(json_part)
        try:
          parsed = json.loads(json_part)
          if not run_id or parsed.get("run_id") == run_id:
            summaries.append(parsed)
        except json.JSONDecodeError:
          pass
  return summaries


def main() -> int:
  parser = argparse.ArgumentParser()
  parser.add_argument("--project", required=True)
  parser.add_argument("--database", required=True)
  parser.add_argument("--function-name", required=True)
  parser.add_argument("--topic-name", required=True)
  parser.add_argument("--subscription-name", required=True)
  parser.add_argument("--load-job-name", required=True)
  parser.add_argument("--subscriber-job-name", required=True)
  parser.add_argument("--start-time", required=True)
  parser.add_argument("--run-id", default="")
  parser.add_argument(
      "--wait-monitoring-seconds",
      type=int,
      default=210,
      help="Maximum seconds to poll for Cloud Monitoring metric ingestion.",
  )
  args = parser.parse_args()

  # Wait briefly for Cloud Logging to ingest the job summary lines
  time.sleep(10)

  # 1. Fetch Load Generator & Subscriber Job summaries from Cloud Logging
  load_summaries = fetch_job_summaries(
      args.project,
      args.load_job_name,
      "LOAD_GENERATOR_SUMMARY:",
      args.start_time,
      args.run_id,
  )
  sub_summaries = fetch_job_summaries(
      args.project,
      args.subscriber_job_name,
      "SUBSCRIBER_SUMMARY:",
      args.start_time,
      args.run_id,
  )

  total_load_writes = sum(
      int(s.get("writes_succeeded", 0)) for s in load_summaries
  )
  total_load_qps = sum(float(s.get("achieved_qps", 0.0)) for s in load_summaries)
  sub_summary = sub_summaries[0] if sub_summaries else {}
  sub_unique_docs = int(sub_summary.get("unique_documents_received", 0))
  sub_raw_msgs = int(sub_summary.get("matching_run_messages", 0))
  sampled_checked = int(sub_summary.get("sampled_docs_checked", 0))
  sampled_matched = int(sub_summary.get("sampled_docs_matched", 0))
  sampled_failed = int(sub_summary.get("sampled_docs_failed", 0))

  # 2. Poll Cloud Monitoring API until Firestore, Cloud Run, and Pub/Sub metrics ingest
  fs_write_ops_metric = 0
  cloud_run_requests_metric = 0
  pubsub_published_metric = 0
  pubsub_sent_metric = 0
  pubsub_acked_metric = 0

  poll_start = time.monotonic()
  while True:
    token = get_access_token()
    end_iso = (
        datetime.datetime.now(datetime.timezone.utc)
        + datetime.timedelta(minutes=1)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    fs_write_ops_metric = query_monitoring_sum(
        args.project,
        token,
        (
            'metric.type="firestore.googleapis.com/document/write_ops_count" '
            f'AND resource.labels.database_id="{args.database}"'
        ),
        args.start_time,
        end_iso,
    )
    cloud_run_requests_metric = query_monitoring_sum(
        args.project,
        token,
        (
            'metric.type="run.googleapis.com/request_count" '
            f'AND resource.labels.service_name="{args.function_name}" '
            'AND metric.labels.response_code_class="2xx"'
        ),
        args.start_time,
        end_iso,
    )
    pubsub_published_metric = query_monitoring_sum(
        args.project,
        token,
        (
            'metric.type="pubsub.googleapis.com/topic/send_message_operation_count" '
            f'AND resource.labels.topic_id="{args.topic_name}"'
        ),
        args.start_time,
        end_iso,
    )
    pubsub_sent_metric = query_monitoring_sum(
        args.project,
        token,
        (
            'metric.type="pubsub.googleapis.com/subscription/sent_message_count" '
            f'AND resource.labels.subscription_id="{args.subscription_name}"'
        ),
        args.start_time,
        end_iso,
    )
    pubsub_acked_metric = query_monitoring_sum(
        args.project,
        token,
        (
            'metric.type="pubsub.googleapis.com/subscription/ack_message_count" '
            f'AND resource.labels.subscription_id="{args.subscription_name}"'
        ),
        args.start_time,
        end_iso,
    )

    elapsed_wait = time.monotonic() - poll_start
    print(
        f"[+{elapsed_wait:.0f}s] Cloud Monitoring ingestion progress: "
        f"firestore_writes={fs_write_ops_metric}, "
        f"cloud_run_2xx={cloud_run_requests_metric}, "
        f"pubsub_published={pubsub_published_metric}, "
        f"pubsub_acked={pubsub_acked_metric} "
        f"(expected ~{total_load_writes})",
        flush=True,
    )
    threshold = max(1, int(total_load_writes * 0.90))
    if (
        fs_write_ops_metric >= threshold
        and cloud_run_requests_metric >= threshold
        and pubsub_published_metric >= threshold
        and pubsub_acked_metric >= threshold
    ):
      break
    if elapsed_wait >= args.wait_monitoring_seconds:
      break
    time.sleep(15)

  report = {
      "load_generator": {
          "tasks_reported": len(load_summaries),
          "writes_succeeded": total_load_writes,
          "combined_achieved_qps": round(total_load_qps, 2),
      },
      "cloud_monitoring_metrics": {
          "firestore_document_write_ops_count": fs_write_ops_metric,
          "cloud_run_2xx_request_count": cloud_run_requests_metric,
          "pubsub_topic_send_message_count": pubsub_published_metric,
          "pubsub_subscription_sent_message_count": pubsub_sent_metric,
          "pubsub_subscription_ack_message_count": pubsub_acked_metric,
      },
      "subscriber_validation": {
          "raw_messages_received": sub_raw_msgs,
          "unique_documents_received": sub_unique_docs,
          "sampled_docs_checked": sampled_checked,
          "sampled_docs_matched": sampled_matched,
          "sampled_docs_failed": sampled_failed,
          "collection_group_counts": sub_summary.get(
              "collection_group_counts", {}
          ),
      },
  }

  print("\n=== END-TO-END METRICS & VALIDATION REPORT ===")
  print(json.dumps(report, indent=2), flush=True)

  if total_load_writes == 0:
    print("ERROR: Load generator reported 0 writes.", file=sys.stderr)
    return 1
  if sub_unique_docs == 0:
    print("ERROR: Subscriber received 0 documents.", file=sys.stderr)
    return 1
  if sampled_checked == 0 or sampled_failed > 0:
    print(
        f"ERROR: Sample validation failed (checked={sampled_checked}, failed={sampled_failed}).",
        file=sys.stderr,
    )
    return 1

  delivery_ratio = sub_unique_docs / total_load_writes
  print(
      f"\nDelivery Ratio (Subscriber Unique Docs / Firestore Writes): "
      f"{sub_unique_docs} / {total_load_writes} ({delivery_ratio * 100:.2f}%)"
  )
  if delivery_ratio < 0.90:
    print(
        f"ERROR: Subscriber processed significantly fewer documents than written ({delivery_ratio:.2%}).",
        file=sys.stderr,
    )
    return 1

  print("SUCCESS: End-to-end pipeline verified!", flush=True)
  return 0


if __name__ == "__main__":
  sys.exit(main())
