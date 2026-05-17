"""Pydantic models matching the Go backend's JSON API contracts.

Each model mirrors the corresponding Go struct with JSON tags.
Duration fields are represented as strings (e.g. "30s", "5m") matching
the Go API's configJSON representation.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class NodeStatus(str, Enum):
    online = "online"
    degraded = "degraded"
    offline = "offline"


class TaskStatus(str, Enum):
    pending = "pending"
    ready = "ready"
    assigned = "assigned"
    running = "running"
    completed = "completed"
    failed = "failed"
    blocked = "blocked"


class LeaseStatus(str, Enum):
    active = "active"
    expired = "expired"
    revoked = "revoked"


class SyncEventType(str, Enum):
    task_created = "task_created"
    task_assigned = "task_assigned"
    task_completed = "task_completed"
    task_failed = "task_failed"


class FederationClusterStatus(str, Enum):
    available = "available"
    unavailable = "unavailable"


# ---------------------------------------------------------------------------
# Node models
# ---------------------------------------------------------------------------

class Node(BaseModel):
    id: str
    name: str
    capabilities: List[str] = []
    status: NodeStatus = NodeStatus.online
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
    load: float = 0.0


class JoinRequest(BaseModel):
    node_name: str
    capabilities: List[str] = []
    endpoint: str = ""


class JoinResponse(BaseModel):
    node_id: str
    status: str = "registered"


class HeartbeatRequest(BaseModel):
    node_id: str


class UpdateCapabilitiesRequest(BaseModel):
    capabilities: List[str]


# ---------------------------------------------------------------------------
# Task models
# ---------------------------------------------------------------------------

class Task(BaseModel):
    id: str
    title: str
    requires: List[str] = []
    depends_on: List[str] = Field(default_factory=list, alias="depends_on")
    priority: int = 3  # 1=highest, 5=lowest
    status: TaskStatus = TaskStatus.pending
    assigned_to: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    version: int = 0
    fail_reason: Optional[str] = None


class SubmitTaskRequest(BaseModel):
    title: str
    requires: List[str] = []
    priority: int = 0


class FailTaskRequest(BaseModel):
    reason: str = "failed"


class SetDependenciesRequest(BaseModel):
    depends_on: List[str]


# ---------------------------------------------------------------------------
# Lease models
# ---------------------------------------------------------------------------

class Lease(BaseModel):
    id: str
    task_id: str
    node_id: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: datetime = Field(default_factory=datetime.utcnow)
    status: LeaseStatus = LeaseStatus.active


class CreateLeaseRequest(BaseModel):
    task_id: str
    node_id: str
    ttl_seconds: int = 60


# ---------------------------------------------------------------------------
# Sync models
# ---------------------------------------------------------------------------

class TaskSync(BaseModel):
    task_id: str
    title: str
    status: str
    assigned_to: Optional[str] = None
    version: int = 0


class SyncMessage(BaseModel):
    version: int = 0
    sender_node: str = ""
    task_state: Optional[TaskSync] = None
    event_type: SyncEventType = SyncEventType.task_created
    timestamp: int = 0


class BatchSyncMessage(BaseModel):
    messages: List[SyncMessage] = []


# ---------------------------------------------------------------------------
# Recovery models
# ---------------------------------------------------------------------------

class RecoveryEvent(BaseModel):
    id: str
    task_id: str = ""
    node_id: str = ""
    action: str = ""  # revoke_lease, reschedule, mark_failed
    status: str = ""  # completed, partial, failed
    message: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class RecoveryTriggerRequest(BaseModel):
    node_id: str


# ---------------------------------------------------------------------------
# Schedule models
# ---------------------------------------------------------------------------

class SchedulingDecision(BaseModel):
    task_id: str
    task_title: str
    priority: int
    node_id: str
    score: float
    reason: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class SchedulingStats(BaseModel):
    total_decisions: int = 0
    decisions_by_priority: Dict[int, int] = {}
    avg_wait_time_ms: float = 0.0
    failed_schedules: int = 0
    failure_reasons: Dict[str, int] = {}
    last_decisions: List[SchedulingDecision] = []


# ---------------------------------------------------------------------------
# Federation models
# ---------------------------------------------------------------------------

class RemoteCluster(BaseModel):
    id: str
    name: str
    endpoint: str
    status: FederationClusterStatus = FederationClusterStatus.available
    registered_at: datetime = Field(default_factory=datetime.utcnow)
    last_ping: datetime = Field(default_factory=datetime.utcnow)
    ping_latency: float = 0.0


class FederationRegisterRequest(BaseModel):
    name: str
    endpoint: str


class FederationForwardRequest(BaseModel):
    cluster_id: str
    title: str
    requires: List[str] = []


# ---------------------------------------------------------------------------
# Hook models
# ---------------------------------------------------------------------------

class EventType(str, Enum):
    task_created = "task_created"
    task_assigned = "task_assigned"
    task_completed = "task_completed"
    task_failed = "task_failed"
    node_offline = "node_offline"


class Hook(BaseModel):
    id: str
    url: str
    events: List[EventType] = []
    secret: Optional[str] = None
    active: bool = True
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class RegisterHookRequest(BaseModel):
    url: str
    events: List[EventType] = []
    secret: Optional[str] = None


class Delivery(BaseModel):
    id: str
    hook_id: str
    event_type: str
    payload: Dict[str, Any] = {}
    status: str = "delivered"
    created_at: datetime = Field(default_factory=datetime.utcnow)


# ---------------------------------------------------------------------------
# Workflow / Dependency models
# ---------------------------------------------------------------------------

class DependencyGraph(BaseModel):
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []


# ---------------------------------------------------------------------------
# Config models (JSON representation with string durations)
# ---------------------------------------------------------------------------

class ClusterConfigJSON(BaseModel):
    id: str = "cluster_default"
    role: str = "main"
    endpoint: str = ""
    token: str = ""


class NodeConfigJSON(BaseModel):
    id: str = "node_main"
    name: str = "main-node"
    capabilities: List[str] = []


class ServerConfigJSON(BaseModel):
    bind: str = "0.0.0.0"
    port: int = 8787


class LeaseConfigJSON(BaseModel):
    ttl: str = "60s"
    scan_rate: str = "10s"


class WatchdogConfigJSON(BaseModel):
    check_interval: str = "5s"
    degraded_after: str = "15s"
    offline_after: str = "30s"


class TLSConfigJSON(BaseModel):
    enabled: bool = False
    cert_file: str = ""
    key_file: str = ""


class HeartbeatConfigJSON(BaseModel):
    interval: str = "30s"
    lease_timeout: str = "120s"


class ReconnectConfigJSON(BaseModel):
    initial_interval: str = "1s"
    max_interval: str = "60s"
    multiplier: float = 2.0


class FederationConfigJSON(BaseModel):
    enabled: bool = True
    ping_interval: str = "30s"
    token: str = ""


class TelemetryConfigJSON(BaseModel):
    enabled: bool = False
    exporter: str = "otlp"
    endpoint: str = ""
    service_name: str = "hermes-cluster"
    sample_rate: float = 1.0
    batch_timeout: str = "5s"


class ConfigJSON(BaseModel):
    cluster: ClusterConfigJSON = ClusterConfigJSON()
    node: NodeConfigJSON = NodeConfigJSON()
    server: ServerConfigJSON = ServerConfigJSON()
    lease: LeaseConfigJSON = LeaseConfigJSON()
    watchdog: WatchdogConfigJSON = WatchdogConfigJSON()
    tls: TLSConfigJSON = TLSConfigJSON()
    heartbeat: HeartbeatConfigJSON = HeartbeatConfigJSON()
    reconnect: ReconnectConfigJSON = ReconnectConfigJSON()
    federation: FederationConfigJSON = FederationConfigJSON()
    telemetry: TelemetryConfigJSON = TelemetryConfigJSON()


# ---------------------------------------------------------------------------
# Status / Summary
# ---------------------------------------------------------------------------

class StatusFilter(BaseModel):
    node: str = ""
    status: str = ""
    capability: str = ""


# ---------------------------------------------------------------------------
# Health / Summary responses
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    status: str = "ok"
    cluster_id: str = ""
    node_id: str = ""
    role: str = ""
    uptime_seconds: int = 0
    version: str = "python-1.0.0"
