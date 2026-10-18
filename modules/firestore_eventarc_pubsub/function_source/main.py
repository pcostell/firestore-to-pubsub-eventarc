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

"""Cloud Run function that forwards Firestore Eventarc change events to Pub/Sub."""

import base64
import datetime
import json
import logging
import os
from typing import Any, Mapping, Optional, Tuple

from cloudevents.http import CloudEvent
import functions_framework
from google.cloud import pubsub_v1
from google.events.cloud import firestore_v1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_publisher_client: Optional[pubsub_v1.PublisherClient] = None


def get_publisher_client() -> pubsub_v1.PublisherClient:
  """Returns a cached Pub/Sub PublisherClient configured for high-throughput batching."""
  global _publisher_client
  if _publisher_client is None:
    batch_settings = pubsub_v1.types.BatchSettings(
        max_messages=250,
        max_bytes=1024 * 1024,
        max_latency=0.01,
    )
    _publisher_client = pubsub_v1.PublisherClient(batch_settings=batch_settings)
  return _publisher_client


def parse_collection_groups(raw_value: str) -> frozenset[str]:
  """Parses a comma-separated or JSON list of collection groups."""
  cleaned = raw_value.strip()
  if not cleaned:
    return frozenset()
  if cleaned.startswith("["):
    try:
      parsed = json.loads(cleaned)
      if isinstance(parsed, list):
        return frozenset(str(item).strip() for item in parsed if str(item).strip())
    except json.JSONDecodeError:
      pass
  return frozenset(part.strip() for part in cleaned.split(",") if part.strip())


def extract_document_path(
    attributes: Mapping[str, Any],
    decoded_payload: Optional[Mapping[str, Any]] = None,
) -> str:
  """Extracts the Firestore document path relative to `/documents/`."""
  doc_attr = str(attributes.get("document") or "")
  if doc_attr:
    return doc_attr.lstrip("/")

  subject = str(attributes.get("subject") or "")
  if subject:
    if subject.startswith("documents/"):
      return subject[len("documents/") :].lstrip("/")
    return subject.lstrip("/")

  if decoded_payload:
    for key in ("value", "oldValue", "old_value"):
      doc_obj = decoded_payload.get(key)
      if isinstance(doc_obj, Mapping):
        full_name = str(doc_obj.get("name") or "")
        marker = "/documents/"
        if marker in full_name:
          return full_name.split(marker, 1)[1].lstrip("/")
  return ""


def extract_collection_group_and_doc_id(document_path: str) -> Tuple[str, str]:
  """Extracts (collection_group, document_id) from a relative Firestore document path."""
  cleaned = document_path.strip("/")
  if not cleaned:
    return "", ""
  segments = cleaned.split("/")
  if len(segments) < 2:
    return "", segments[0]
  return segments[-2], segments[-1]


def infer_operation_type(decoded_payload: Optional[Mapping[str, Any]]) -> str:
  """Infers whether the write event was a CREATE, UPDATE, or DELETE."""
  if not decoded_payload:
    return "WRITE"
  has_value = bool(decoded_payload.get("value"))
  has_old_value = bool(
      decoded_payload.get("oldValue") or decoded_payload.get("old_value")
  )
  if has_value and not has_old_value:
    return "CREATE"
  if has_value and has_old_value:
    return "UPDATE"
  if not has_value and has_old_value:
    return "DELETE"
  return "WRITE"


def decode_event_data(raw_data: Any) -> Tuple[Optional[dict[str, Any]], bytes]:
  """Decodes CloudEvent data (Protobuf or JSON) into a dictionary and JSON bytes."""
  if isinstance(raw_data, (bytes, bytearray)):
    raw_bytes = bytes(raw_data)
    if not raw_bytes:
      return {}, b"{}"
    if raw_bytes.lstrip().startswith(b"{"):
      try:
        parsed = json.loads(raw_bytes.decode("utf-8"))
        if isinstance(parsed, dict):
          return parsed, json.dumps(parsed).encode("utf-8")
      except (UnicodeDecodeError, json.JSONDecodeError):
        pass
    try:
      doc_event = firestore_v1.DocumentEventData.deserialize(raw_bytes)
      json_str = firestore_v1.DocumentEventData.to_json(
          doc_event,
          including_default_value_fields=False,
          preserving_proto_field_name=False,
      )
      parsed = json.loads(json_str)
      return parsed, json_str.encode("utf-8")
    except Exception as exc:  # pylint: disable=broad-except
      logger.warning(
          "Failed to deserialize protobuf DocumentEventData; encoding raw bytes as base64: %s",
          exc,
      )
      fallback = {
          "rawProtobufBase64": base64.b64encode(raw_bytes).decode("ascii")
      }
      return fallback, json.dumps(fallback).encode("utf-8")

  if isinstance(raw_data, str):
    try:
      parsed = json.loads(raw_data)
      if isinstance(parsed, dict):
        return parsed, json.dumps(parsed).encode("utf-8")
    except json.JSONDecodeError:
      pass
    wrapped = {"rawData": raw_data}
    return wrapped, json.dumps(wrapped).encode("utf-8")

  if isinstance(raw_data, Mapping):
    parsed = dict(raw_data)
    return parsed, json.dumps(parsed).encode("utf-8")

  return {}, b"{}"


@functions_framework.cloud_event
def handle_firestore_event(cloud_event: CloudEvent) -> None:
  """Handles an incoming Firestore CloudEvent from Eventarc and publishes it to Pub/Sub."""
  topic_path = os.environ.get("PUBSUB_TOPIC_PATH", "").strip()
  if not topic_path:
    raise RuntimeError("Environment variable PUBSUB_TOPIC_PATH is required.")

  configured_database = os.environ.get("FIRESTORE_DATABASE", "(default)").strip()
  allowed_collection_groups = parse_collection_groups(
      os.environ.get("COLLECTION_GROUPS", "")
  )
  payload_format = (
      os.environ.get("PUBSUB_PAYLOAD_FORMAT", "json").strip().lower()
  )

  attributes = cloud_event.get_attributes()
  raw_data = cloud_event.data

  decoded_dict, decoded_json_bytes = decode_event_data(raw_data)
  document_path = extract_document_path(attributes, decoded_dict)
  collection_group, document_id = extract_collection_group_and_doc_id(
      document_path
  )

  if (
      allowed_collection_groups
      and collection_group not in allowed_collection_groups
  ):
    return

  operation_type = infer_operation_type(decoded_dict)
  event_database = str(attributes.get("database") or configured_database)
  event_time = str(
      attributes.get("time")
      or datetime.datetime.now(datetime.timezone.utc).isoformat()
  )

  if payload_format == "protobuf" and isinstance(raw_data, (bytes, bytearray)):
    message_bytes = bytes(raw_data)
    content_type = "application/protobuf"
  else:
    envelope = {
        "id": str(attributes.get("id") or ""),
        "type": str(attributes.get("type") or ""),
        "source": str(attributes.get("source") or ""),
        "subject": str(attributes.get("subject") or ""),
        "time": event_time,
        "database": event_database,
        "document": document_path,
        "collectionGroup": collection_group,
        "documentId": document_id,
        "operationType": operation_type,
        "data": (
            decoded_dict
            if decoded_dict is not None
            else json.loads(decoded_json_bytes.decode("utf-8"))
        ),
    }
    message_bytes = json.dumps(envelope).encode("utf-8")
    content_type = "application/json"

  pubsub_attributes = {
      "ce_id": str(attributes.get("id") or ""),
      "ce_type": str(attributes.get("type") or ""),
      "ce_source": str(attributes.get("source") or ""),
      "ce_subject": str(attributes.get("subject") or ""),
      "ce_time": event_time,
      "content_type": content_type,
      "database": event_database,
      "document": document_path,
      "collection_group": collection_group,
      "document_id": document_id,
      "operation_type": operation_type,
  }
  sanitized_attributes = {
      k: v[:1024] for k, v in pubsub_attributes.items() if v
  }

  publisher = get_publisher_client()
  future = publisher.publish(
      topic_path, data=message_bytes, **sanitized_attributes
  )
  future.result(timeout=30)
