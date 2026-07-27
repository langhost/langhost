"""SQLAlchemy ORM models; schema DDL is applied via Alembic."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

_JSONB_EMPTY = text("'{}'::jsonb")
_NOW = text("now()")


class Base(DeclarativeBase):
    pass


class AssistantRow(Base):
    __tablename__ = "assistants"
    __table_args__ = (
        Index("ix_assistants_graph_id", "graph_id"),
        Index("ix_assistants_created_at", "created_at"),
        Index(
            "ix_assistants_metadata_gin",
            "metadata",
            postgresql_using="gin",
            postgresql_ops={"metadata": "jsonb_path_ops"},
        ),
    )

    assistant_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    graph_id: Mapped[str] = mapped_column(String(256), nullable=False)
    name: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    config: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=_JSONB_EMPTY)
    context: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=_JSONB_EMPTY)
    metadata_: Mapped[dict] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=_JSONB_EMPTY
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=_NOW
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=_NOW
    )


class AssistantVersionRow(Base):
    __tablename__ = "assistant_versions"

    assistant_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    graph_id: Mapped[str] = mapped_column(String(256), nullable=False)
    config: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=_JSONB_EMPTY)
    context: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=_JSONB_EMPTY)
    metadata_: Mapped[dict] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=_JSONB_EMPTY
    )
    name: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=_NOW
    )


class ThreadRow(Base):
    __tablename__ = "threads"
    __table_args__ = (
        Index("ix_threads_status_updated_at", "status", "updated_at"),
        Index("ix_threads_updated_at", "updated_at"),
        Index(
            "ix_threads_metadata_gin",
            "metadata",
            postgresql_using="gin",
            postgresql_ops={"metadata": "jsonb_path_ops"},
        ),
    )

    thread_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="idle")
    metadata_: Mapped[dict] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=_JSONB_EMPTY
    )
    config: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=_JSONB_EMPTY)
    values_: Mapped[dict | None] = mapped_column("values", JSONB, nullable=True)
    interrupts: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=_JSONB_EMPTY)
    error: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=_NOW
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=_NOW
    )
    state_updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class RunRow(Base):
    __tablename__ = "runs"
    __table_args__ = (
        Index("ix_runs_status_created_at", "status", "created_at"),
        Index("ix_runs_thread_id_status", "thread_id", "status"),
        Index("ix_runs_thread_id_created_at", "thread_id", "created_at"),
        Index("ix_runs_assistant_id_status", "assistant_id", "status"),
        Index(
            "uq_runs_one_running_per_thread",
            "thread_id",
            unique=True,
            postgresql_where=text("status = 'running' AND thread_id IS NOT NULL"),
        ),
    )

    run_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    thread_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    assistant_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    metadata_: Mapped[dict] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=_JSONB_EMPTY
    )
    kwargs: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=_JSONB_EMPTY)
    multitask_strategy: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=_NOW
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=_NOW
    )


class CronRow(Base):
    __tablename__ = "crons"
    __table_args__ = (
        Index("ix_crons_enabled_next_run", "enabled", "next_run_date"),
        Index("ix_crons_assistant_id", "assistant_id"),
        Index("ix_crons_thread_id", "thread_id"),
        Index(
            "ix_crons_metadata_gin",
            "metadata",
            postgresql_using="gin",
            postgresql_ops={"metadata": "jsonb_path_ops"},
        ),
    )

    cron_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    assistant_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    thread_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True)
    schedule: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=_JSONB_EMPTY)
    metadata_: Mapped[dict] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=_JSONB_EMPTY
    )
    next_run_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    end_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    user_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    timezone: Mapped[str | None] = mapped_column(String(64), nullable=True)
    on_run_completed: Mapped[str | None] = mapped_column(String(16), nullable=True)
    enabled: Mapped[bool] = mapped_column(nullable=False, default=True, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=_NOW
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=_NOW
    )


class StoreItemRow(Base):
    __tablename__ = "store_items"

    prefix: Mapped[str] = mapped_column(Text, primary_key=True)
    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default=_JSONB_EMPTY)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=_NOW
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=_NOW
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class RetryCounterRow(Base):
    __tablename__ = "retry_counters"

    run_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True)
    count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
