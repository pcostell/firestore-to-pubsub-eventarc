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

"""Pub/Sub subscriber job that consumes Firestore change events and validates a sample against Firestore."""

from concurrent import futures
import hashlib
import json
import logging
import os
import random
import threading
import time
from typing import Any, Mapping

import google.auth
import google.auth.transport.requests
from google.cloud import firestore
from google.cloud import pubsub_v1
import requests
import requests.adapters

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def compute_payload_hash(
    run_id: str, collection_group: str, doc_id: str, seq: int
) -> str:
  raw = f"{run_id}:{collection_group}:{doc_id}:{seq}".encode("utf-8")
  return hashlib.sha256(raw).hexdigest()


def extract_firestore_proto_fields(
    fields_map: Mapping[str, Any],
) -> dict[str, Any]:
  """Extracts plain Python values from Firestore DocumentEventData JSON fields."""
  result: dict[str, Any] = {}
  for key, val_obj in fields_map.items():
    if not isinstance(val_obj, Mapping):
      continue
    if "stringValue" in val_obj:
      result[key] = str(val_obj["stringValue"])
    elif "integerValue" in val_obj:
      result[key] = int(val_obj["integerValue"])
    elif "doubleValue" in val_obj:
      result[key] = float(val_obj["doubleValue"])
    elif "booleanValue" in val_obj:
      result[key] = bool(val_obj["booleanValue"])
  return result


def main() -> None:
  project_id = os.environ["PROJECT_ID"]
  database_id = os.environ.get("FIRESTORE_DATABASE", "(default)")
  subscription_path = os.environ["PUBSUB_SUBSCRIPTION_PATH"]
  expected_run_id = os.environ.get("RUN_ID", "")
  sample_rate = float(os.environ.get("SAMPLE_RATE", "0.05"))
  max_sampled_docs = int(os.environ.get("MAX_SAMPLED_DOCS", "500"))
  idle_timeout_seconds = float(os.environ.get("IDLE_TIMEOUT_SECONDS", "25"))
  max_runtime_seconds = float(os.environ.get("MAX_RUNTIME_SECONDS", "180"))

  logger.info(
      "Starting Pub/Sub Subscriber & Validator against subscription=%s "
      "database=%s run_id=%s sample_rate=%.3f max_sampled_docs=%d "
      "idle_timeout=%.1fs max_runtime=%.1fs",
      subscription_path,
      database_id,
      expected_run_id,
      sample_rate,
      max_sampled_docs,
      idle_timeout_seconds,
      max_runtime_seconds,
  )

  session = requests.Session()
  adapter = requests.adapters.HTTPAdapter(
      pool_connections=64, pool_maxsize=64, max_retries=3
  )
  session.mount("http://", adapter)
  session.mount("https://", adapter)
  auth_req = google.auth.transport.requests.Request(session=session)
  creds, _ = google.auth.default(
      scopes=["https://www.googleapis.com/auth/cloud-platform"]
  )
  creds.refresh(auth_req)

  fs_client = firestore.Client(
      project=project_id, database=database_id, credentials=creds
  )
  subscriber = pubsub_v1.SubscriberClient(credentials=creds)
  validation_executor = futures.ThreadPoolExecutor(max_workers=32)

  lock = threading.Lock()
  start_time = time.monotonic()
  last_message_time = time.monotonic()

  raw_messages_received = 0
  matching_run_messages = 0
  unique_documents: set[str] = set()
  envelope_valid_count = 0
  envelope_invalid_count = 0
  sampled_docs_checked = 0
  sampled_docs_matched = 0
  sampled_docs_failed = 0
  collection_group_counts: dict[str, int] = {}
  validation_futures: list[futures.Future[None]] = []

  def verify_against_firestore(
      doc_path: str, expected_fields: Mapping[str, Any]
  ) -> None:
    nonlocal sampled_docs_checked, sampled_docs_matched, sampled_docs_failed
    try:
      snap = fs_client.document(doc_path).get()
      if not snap.exists:
        with lock:
          sampled_docs_checked += 1
          sampled_docs_failed += 1
        logger.warning(
            "Sampled document %s does not exist in Firestore!", doc_path
        )
        return
      live_data = snap.to_dict() or {}
      matches = all(
          live_data.get(k) == expected_fields.get(k)
          for k in (
              "doc_id",
              "collection_group",
              "run_id",
              "seq",
              "payload_hash",
          )
      )
      with lock:
        sampled_docs_checked += 1
        if matches:
          sampled_docs_matched += 1
        else:
          sampled_docs_failed += 1
          if sampled_docs_failed <= 5:
            logger.warning(
                "Sampled document mismatch for %s: live=%s vs event=%s",
                doc_path,
                live_data,
                expected_fields,
            )
    except Exception as exc:  # pylint: disable=broad-except
      with lock:
        sampled_docs_checked += 1
        sampled_docs_failed += 1
      logger.warning(
          "Error validating sampled document %s: %s", doc_path, exc
      )

  def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    nonlocal raw_messages_received, matching_run_messages, last_message_time
    nonlocal envelope_valid_count, envelope_invalid_count

    message.ack()
    now = time.monotonic()

    try:
      envelope = json.loads(message.data.decode("utf-8"))
      doc_path = str(
          envelope.get("document") or message.attributes.get("document") or ""
      )
      cg = str(
          envelope.get("collectionGroup")
          or message.attributes.get("collection_group")
          or ""
      )
      doc_id = str(
          envelope.get("documentId")
          or message.attributes.get("document_id")
          or ""
      )
      value_obj = (envelope.get("data") or {}).get("value") or {}
      fields = extract_firestore_proto_fields(value_obj.get("fields") or {})
      msg_run_id = str(fields.get("run_id") or "")

      with lock:
        raw_messages_received += 1
        last_message_time = now

      if expected_run_id and msg_run_id and msg_run_id != expected_run_id:
        return

      seq = int(fields.get("seq") or 0)
      expected_hash = compute_payload_hash(msg_run_id, cg, doc_id, seq)
      actual_hash = str(fields.get("payload_hash") or "")
      is_valid = (
          bool(doc_path)
          and bool(cg)
          and bool(doc_id)
          and actual_hash == expected_hash
      )

      should_sample = False
      with lock:
        matching_run_messages += 1
        unique_documents.add(doc_path)
        collection_group_counts[cg] = collection_group_counts.get(cg, 0) + 1
        if is_valid:
          envelope_valid_count += 1
        else:
          envelope_invalid_count += 1
        if (
            is_valid
            and len(validation_futures) < max_sampled_docs
            and random.random() < sample_rate
        ):
          should_sample = True

      if should_sample:
        fut = validation_executor.submit(
            verify_against_firestore, doc_path, fields
        )
        with lock:
          validation_futures.append(fut)

    except Exception as exc:  # pylint: disable=broad-except
      with lock:
        raw_messages_received += 1
        envelope_invalid_count += 1
        last_message_time = now
      logger.warning("Failed to parse Pub/Sub message: %s", exc)

  flow_control = pubsub_v1.types.FlowControl(max_messages=5000)
  streaming_pull_future = subscriber.subscribe(
      subscription_path,
      callback=callback,
      flow_control=flow_control,
  )

  try:
    while True:
      time.sleep(2.0)
      now = time.monotonic()
      elapsed = now - start_time
      with lock:
        idle = now - last_message_time
        matched = matching_run_messages
        unique_cnt = len(unique_documents)
        sampled_cnt = sampled_docs_checked

      logger.info(
          "Subscriber progress: raw=%d matched=%d unique_docs=%d "
          "sampled_checked=%d idle=%.1fs elapsed=%.1fs",
          raw_messages_received,
          matched,
          unique_cnt,
          sampled_cnt,
          idle,
          elapsed,
      )

      if matched > 0 and idle >= idle_timeout_seconds:
        logger.info(
            "Subscription idle for %.1fs after receiving %d messages; finishing.",
            idle,
            matched,
        )
        break
      if elapsed >= max_runtime_seconds:
        logger.info(
            "Reached max_runtime_seconds (%.1fs); finishing.",
            max_runtime_seconds,
        )
        break
  finally:
    streaming_pull_future.cancel()
    try:
      streaming_pull_future.result(timeout=10)
    except Exception:  # pylint: disable=broad-except
      pass

  futures.wait(validation_futures, timeout=30)
  validation_executor.shutdown(wait=True)
  subscriber.close()

  elapsed = max(0.001, time.monotonic() - start_time)
  summary = {
      "run_id": expected_run_id,
      "raw_messages_received": raw_messages_received,
      "matching_run_messages": matching_run_messages,
      "unique_documents_received": len(unique_documents),
      "envelope_valid_count": envelope_valid_count,
      "envelope_invalid_count": envelope_invalid_count,
      "sampled_docs_checked": sampled_docs_checked,
      "sampled_docs_matched": sampled_docs_matched,
      "sampled_docs_failed": sampled_docs_failed,
      "collection_group_counts": collection_group_counts,
      "elapsed_seconds": round(elapsed, 2),
  }
  print(f"SUBSCRIBER_SUMMARY: {json.dumps(summary)}", flush=True)


if __name__ == "__main__":
  main()
