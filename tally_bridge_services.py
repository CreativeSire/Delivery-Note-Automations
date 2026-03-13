from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from flask import current_app
from sqlalchemy import func, select
from werkzeug.utils import secure_filename

from models import TallyBridgeProfile, TallyDiagnosticsArtifact, TallyDiagnosticsRun, db, utcnow
from services import ServiceError, _string_value

CONNECTION_MODE_OPTIONS = [
    ("manual_fallback", "Manual fallback"),
    ("file_drop", "File drop"),
    ("hybrid", "Hybrid"),
    ("xml_http", "XML / HTTP"),
]

YES_NO_UNKNOWN_OPTIONS = [
    ("unknown", "Unknown"),
    ("yes", "Yes"),
    ("no", "No"),
]

CASE_STATUS_OPTIONS = [
    ("missing", "Missing"),
    ("uploaded", "Uploaded"),
    ("reviewed", "Reviewed"),
    ("linked", "Linked"),
    ("unlinked", "Unlinked"),
]

RUN_STATUS_OPTIONS = [
    ("draft", "Draft"),
    ("evidence_uploaded", "Evidence uploaded"),
    ("manual_case_ready", "Manual case ready"),
    ("upload_case_ready", "Upload case ready"),
    ("compared", "Compared"),
    ("bridge_mode_decided", "Bridge mode decided"),
]

ARTIFACT_GROUP_OPTIONS = [
    ("manual_linked", "Manual linked case"),
    ("uploaded_unlinked", "Uploaded unlinked case"),
    ("environment", "Environment / settings"),
    ("other", "Other"),
]

ARTIFACT_TYPE_OPTIONS = [
    ("sales_order", "Sales Order voucher"),
    ("delivery_note", "Delivery Note voucher"),
    ("sales_invoice", "Sales Invoice voucher"),
    ("register_export", "Register export"),
    ("xml_dump", "XML dump"),
    ("settings_export", "Settings / config"),
    ("screenshot", "Screenshot"),
    ("other", "Other"),
]


@dataclass
class TallyBridgeSummary:
    profiles: list[TallyBridgeProfile]
    active_profile: TallyBridgeProfile | None
    latest_run: TallyDiagnosticsRun | None
    run_count: int
    open_run_count: int
    artifact_count: int
    recent_runs: list[TallyDiagnosticsRun]


@dataclass
class TallyDiagnosticsArtifactGroup:
    code: str
    label: str
    artifacts: list[TallyDiagnosticsArtifact]


@dataclass
class TallyDiagnosticsDetail:
    run: TallyDiagnosticsRun
    artifact_groups: list[TallyDiagnosticsArtifactGroup]
    artifact_count: int
    recommended_mode_label: str


def build_tally_bridge_summary() -> TallyBridgeSummary:
    profiles = list(db.session.scalars(select(TallyBridgeProfile).order_by(TallyBridgeProfile.is_active.desc(), TallyBridgeProfile.name.asc())))
    active_profile = next((profile for profile in profiles if profile.is_active), profiles[0] if profiles else None)
    latest_run = db.session.scalar(select(TallyDiagnosticsRun).order_by(TallyDiagnosticsRun.created_at.desc()).limit(1))
    run_count = db.session.scalar(select(func.count(TallyDiagnosticsRun.id))) or 0
    open_run_count = db.session.scalar(
        select(func.count(TallyDiagnosticsRun.id)).where(TallyDiagnosticsRun.status.not_in(["bridge_mode_decided"]))
    ) or 0
    artifact_count = db.session.scalar(select(func.count(TallyDiagnosticsArtifact.id))) or 0
    recent_runs = list(db.session.scalars(select(TallyDiagnosticsRun).order_by(TallyDiagnosticsRun.created_at.desc()).limit(6)))
    return TallyBridgeSummary(
        profiles=profiles,
        active_profile=active_profile,
        latest_run=latest_run,
        run_count=run_count,
        open_run_count=open_run_count,
        artifact_count=artifact_count,
        recent_runs=recent_runs,
    )


def build_tally_diagnostics_detail(run_id: str) -> TallyDiagnosticsDetail | None:
    run = db.session.get(TallyDiagnosticsRun, run_id)
    if run is None:
        return None

    artifacts = list(
        db.session.scalars(
            select(TallyDiagnosticsArtifact)
            .where(TallyDiagnosticsArtifact.run_id == run_id)
            .order_by(TallyDiagnosticsArtifact.created_at.desc(), TallyDiagnosticsArtifact.id.desc())
        )
    )
    groups: list[TallyDiagnosticsArtifactGroup] = []
    for code, label in ARTIFACT_GROUP_OPTIONS:
        groups.append(
            TallyDiagnosticsArtifactGroup(
                code=code,
                label=label,
                artifacts=[artifact for artifact in artifacts if artifact.artifact_group == code],
            )
        )
    grouped_ids = {artifact.id for group in groups for artifact in group.artifacts}
    other_artifacts = [artifact for artifact in artifacts if artifact.id not in grouped_ids]
    if other_artifacts:
        groups.append(TallyDiagnosticsArtifactGroup(code="unclassified", label="Unclassified", artifacts=other_artifacts))

    recommended_mode = derive_tally_bridge_mode(run)
    return TallyDiagnosticsDetail(
        run=run,
        artifact_groups=groups,
        artifact_count=len(artifacts),
        recommended_mode_label=_mode_label(recommended_mode),
    )


def get_tally_bridge_profile(profile_id: int) -> TallyBridgeProfile | None:
    return db.session.get(TallyBridgeProfile, profile_id)


def get_tally_diagnostics_run(run_id: str) -> TallyDiagnosticsRun | None:
    return db.session.get(TallyDiagnosticsRun, run_id)


def get_tally_diagnostics_artifact(artifact_id: int) -> TallyDiagnosticsArtifact | None:
    return db.session.get(TallyDiagnosticsArtifact, artifact_id)


def save_tally_bridge_profile(values: dict[str, str], *, profile_id: int | None = None) -> TallyBridgeProfile:
    profile = db.session.get(TallyBridgeProfile, profile_id) if profile_id else None
    if profile is None:
        profile = TallyBridgeProfile()
        db.session.add(profile)

    name = _string_value(values.get("name"))
    if not name:
        raise ServiceError("Please give this Tally profile a name first.")

    connection_mode = _choice_value(values.get("connection_mode"), CONNECTION_MODE_OPTIONS, "manual_fallback")
    is_active = _string_value(values.get("is_active")).lower() != "no"

    profile.name = name
    profile.connection_mode = connection_mode
    profile.company_name = _nullable_text(values.get("company_name"))
    profile.tally_version = _nullable_text(values.get("tally_version"))
    profile.endpoint_url = _nullable_text(values.get("endpoint_url"))
    profile.machine_name = _nullable_text(values.get("machine_name"))
    profile.notes = _nullable_text(values.get("notes"))
    profile.is_active = is_active

    profile.capabilities_json = {
        "xml_http": _choice_value(values.get("profile_xml_http"), YES_NO_UNKNOWN_OPTIONS, "unknown"),
        "outbound_import": _choice_value(values.get("profile_outbound_import"), YES_NO_UNKNOWN_OPTIONS, "unknown"),
        "register_fetch": _choice_value(values.get("profile_register_fetch"), YES_NO_UNKNOWN_OPTIONS, "unknown"),
    }
    if any(value != "unknown" for value in profile.capabilities_json.values()):
        profile.last_checked_at = utcnow()

    if is_active:
        db.session.query(TallyBridgeProfile).filter(TallyBridgeProfile.id != profile.id).update(
            {"is_active": False}, synchronize_session=False
        )

    db.session.commit()
    return profile


def create_tally_diagnostics_run(values: dict[str, str]) -> TallyDiagnosticsRun:
    title = _string_value(values.get("title"))
    if not title:
        raise ServiceError("Please name this Tally diagnostics run first.")

    profile = _resolve_profile(values.get("profile_id"))
    run = TallyDiagnosticsRun(
        id=uuid4().hex,
        profile_id=profile.id if profile is not None else None,
        title=title,
        status="draft",
        notes=_nullable_text(values.get("notes")),
    )
    run.recommended_mode = derive_tally_bridge_mode(run)
    db.session.add(run)
    db.session.commit()
    return run


def update_tally_diagnostics_run(run_id: str, values: dict[str, str]) -> TallyDiagnosticsRun:
    run = db.session.get(TallyDiagnosticsRun, run_id)
    if run is None:
        raise ServiceError("This Tally diagnostics run could not be found.")

    run.status = _choice_value(values.get("status"), RUN_STATUS_OPTIONS, run.status)
    run.xml_http_supported = _choice_value(values.get("xml_http_supported"), YES_NO_UNKNOWN_OPTIONS, run.xml_http_supported)
    run.outbound_import_supported = _choice_value(
        values.get("outbound_import_supported"), YES_NO_UNKNOWN_OPTIONS, run.outbound_import_supported
    )
    run.register_fetch_supported = _choice_value(
        values.get("register_fetch_supported"), YES_NO_UNKNOWN_OPTIONS, run.register_fetch_supported
    )
    run.dn_link_supported = _choice_value(values.get("dn_link_supported"), YES_NO_UNKNOWN_OPTIONS, run.dn_link_supported)
    run.manual_case_status = _choice_value(values.get("manual_case_status"), CASE_STATUS_OPTIONS, run.manual_case_status)
    run.uploaded_case_status = _choice_value(
        values.get("uploaded_case_status"), CASE_STATUS_OPTIONS, run.uploaded_case_status
    )
    run.findings_summary = _nullable_text(values.get("findings_summary"))
    run.notes = _nullable_text(values.get("notes"))
    run.recommended_mode = derive_tally_bridge_mode(run)
    if run.status == "bridge_mode_decided":
        run.completed_at = utcnow()

    db.session.commit()
    return run


def add_tally_diagnostics_artifact(
    run_id: str,
    *,
    file_storage: object,
    artifact_group: str,
    artifact_type: str,
    description: str | None = None,
) -> TallyDiagnosticsArtifact:
    run = db.session.get(TallyDiagnosticsRun, run_id)
    if run is None:
        raise ServiceError("This Tally diagnostics run could not be found.")

    filename = _string_value(getattr(file_storage, "filename", ""))
    if not filename:
        raise ServiceError("Please choose a Tally artifact file first.")

    payload = file_storage.read()
    if hasattr(file_storage, "stream"):
        file_storage.stream.seek(0)
    if not payload:
        raise ServiceError("The selected artifact file is empty.")

    group_code = _choice_value(artifact_group, ARTIFACT_GROUP_OPTIONS, "other")
    type_code = _choice_value(artifact_type, ARTIFACT_TYPE_OPTIONS, "other")
    safe_name = secure_filename(filename) or f"artifact-{uuid4().hex}"
    stored_name = f"{uuid4().hex}-{safe_name}"
    target_dir = Path(current_app.instance_path) / "tally_bridge" / run.id
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / stored_name
    target_path.write_bytes(payload)

    artifact = TallyDiagnosticsArtifact(
        run_id=run.id,
        artifact_group=group_code,
        artifact_type=type_code,
        filename=filename,
        content_type=_string_value(getattr(file_storage, "mimetype", "")) or None,
        storage_path=str(target_path),
        description=_nullable_text(description),
        file_size=len(payload),
    )
    db.session.add(artifact)

    if group_code == "manual_linked" and run.manual_case_status == "missing":
        run.manual_case_status = "uploaded"
    if group_code == "uploaded_unlinked" and run.uploaded_case_status == "missing":
        run.uploaded_case_status = "uploaded"
    if run.status == "draft":
        run.status = "evidence_uploaded"

    db.session.commit()
    return artifact


def derive_tally_bridge_mode(run: TallyDiagnosticsRun) -> str:
    if (
        run.xml_http_supported == "yes"
        and run.outbound_import_supported == "yes"
        and run.register_fetch_supported == "yes"
        and run.dn_link_supported == "yes"
    ):
        return "xml_http"
    if run.outbound_import_supported == "yes" and run.register_fetch_supported == "yes":
        return "hybrid"
    if run.outbound_import_supported == "yes":
        return "file_drop"
    return "manual_fallback"


def _resolve_profile(raw_profile_id: str | None) -> TallyBridgeProfile | None:
    text = _string_value(raw_profile_id)
    if text.isdigit():
        profile = db.session.get(TallyBridgeProfile, int(text))
        if profile is not None:
            return profile
    return db.session.scalar(select(TallyBridgeProfile).where(TallyBridgeProfile.is_active.is_(True)).limit(1))


def _nullable_text(value: object) -> str | None:
    text = _string_value(value)
    return text or None


def _choice_value(value: object, options: list[tuple[str, str]], default: str) -> str:
    selected = _string_value(value)
    allowed = {code for code, _ in options}
    return selected if selected in allowed else default


def _mode_label(code: str) -> str:
    options = dict(CONNECTION_MODE_OPTIONS)
    return options.get(code, code.replace("_", " ").title())
