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

"""High-throughput Firestore load generator (up to 1000+ QPS)."""

from concurrent import futures
import hashlib
import json
import logging
import os
import threading
import time
import uuid

import google.auth
import google.auth.transport.requests
from google.cloud import firestore
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


def create_warmed_clients(
    project_id: str, database_id: str, num_clients: int = 4
) -> list[firestore.Client]:
  """Creates Firestore clients with pre-refreshed credentials and large HTTP pool."""
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

  clients = [
      firestore.Client(
          project=project_id, database=database_id, credentials=creds
      )
      for _ in range(num_clients)
  ]
  return clients


def run_load() -> None:
  project_id = os.environ["PROJECT_ID"]
  database_id = os.environ.get("FIRESTORE_DATABASE", "(default)")
  total_target_qps = float(os.environ.get("TARGET_QPS", "1000"))
  duration_seconds = float(os.environ.get("DURATION_SECONDS", "20"))
  batch_size = max(1, int(os.environ.get("BATCH_SIZE", "5")))
  run_id = os.environ.get("RUN_ID", "default-run")
  collection_groups_raw = os.environ.get(
      "COLLECTION_GROUPS", "orders,inventory"
  )
  collection_groups = [
      c.strip() for c in collection_groups_raw.split(",") if c.strip()
  ] or ["orders"]

  task_index = int(os.environ.get("CLOUD_RUN_TASK_INDEX", "0"))
  task_count = max(1, int(os.environ.get("CLOUD_RUN_TASK_COUNT", "1")))
  per_task_qps = total_target_qps / task_count
  num_workers = min(48, max(16, int(per_task_qps / batch_size)))

  logger.info(
      "Starting Load Generator task %d/%d against project=%s database=%s "
      "total_target_qps=%.1f per_task_qps=%.1f duration=%.1fs workers=%d "
      "batch_size=%d collection_groups=%s run_id=%s",
      task_index,
      task_count,
      project_id,
      database_id,
      total_target_qps,
      per_task_qps,
      duration_seconds,
      num_workers,
      batch_size,
      collection_groups,
      run_id,
  )

  clients = create_warmed_clients(project_id, database_id, num_clients=4)

  writes_succeeded = 0
  writes_failed = 0
  seq_counter = 0
  lock = threading.Lock()
  start_time = time.monotonic()
  end_time = start_time + duration_seconds

  def worker(worker_id: int) -> None:
    nonlocal writes_succeeded, writes_failed, seq_counter
    client = clients[worker_id % len(clients)]
    worker_doc_qps = per_task_qps / num_workers
    batch_interval = (
        (batch_size / worker_doc_qps) if worker_doc_qps > 0 else 0.0
    )
    next_fire = time.monotonic() + (
        worker_id * batch_interval / max(1, num_workers)
    )

    while True:
      now = time.monotonic()
      if now >= end_time:
        break
      if now < next_fire:
        time.sleep(next_fire - now)
      next_fire += batch_interval

      with lock:
        base_seq = seq_counter + 1
        seq_counter += batch_size

      batch = client.batch()
      for offset in range(batch_size):
        seq = base_seq + offset
        cg = collection_groups[seq % len(collection_groups)]
        rand_prefix = uuid.uuid4().hex[:12]
        doc_id = f"{rand_prefix}-t{task_index}-s{seq}"
        if seq % 2 == 0:
          doc_path = f"{cg}/{doc_id}"
        else:
          parent_id = f"tenant-{rand_prefix[:6]}"
          doc_path = f"tenants/{parent_id}/{cg}/{doc_id}"

        payload_hash = compute_payload_hash(run_id, cg, doc_id, seq)
        payload = {
            "doc_id": doc_id,
            "collection_group": cg,
            "run_id": run_id,
            "task_index": task_index,
            "seq": seq,
            "payload_hash": payload_hash,
            "created_at_ms": int(time.time() * 1000),
        }
        batch.set(client.document(doc_path), payload)

      try:
        batch.commit(timeout=15.0)
        with lock:
          writes_succeeded += batch_size
      except Exception as exc:  # pylint: disable=broad-except
        with lock:
          writes_failed += batch_size
          failed_so_far = writes_failed
        if failed_so_far <= 25:
          logger.warning("Firestore batch write failed: %s", exc)

  stop_reporter = threading.Event()

  def progress_reporter() -> None:
    while not stop_reporter.wait(5.0):
      elapsed = max(0.001, time.monotonic() - start_time)
      with lock:
        succ = writes_succeeded
        fail = writes_failed
      logger.info(
          "Task %d progress: succeeded=%d failed=%d rate=%.1f QPS",
          task_index,
          succ,
          fail,
          succ / elapsed,
      )

  reporter_thread = threading.Thread(target=progress_reporter, daemon=True)
  reporter_thread.start()

  with futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
    worker_futures = [executor.submit(worker, i) for i in range(num_workers)]
    futures.wait(worker_futures)

  stop_reporter.set()
  elapsed = max(0.001, time.monotonic() - start_time)
  achieved_qps = writes_succeeded / elapsed
  summary = {
      "task_index": task_index,
      "task_count": task_count,
      "run_id": run_id,
      "writes_succeeded": writes_succeeded,
      "writes_failed": writes_failed,
      "elapsed_seconds": round(elapsed, 2),
      "achieved_qps": round(achieved_qps, 2),
  }
  print(f"LOAD_GENERATOR_SUMMARY: {json.dumps(summary)}", flush=True)
  for c in clients:
    c.close()


if __name__ == "__main__":
  run_load()
