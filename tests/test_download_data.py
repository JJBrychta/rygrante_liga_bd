"""Unit tests for the manifest-driven Cloudflare R2 dataset downloader."""

from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path

import pytest

from scripts.download_data import (
    REQUIRED_DATASETS,
    compute_sha256,
    download_from_r2,
    load_dotenv_if_present,
    parse_sha256_manifest,
    verify_landing_folders,
)


class _FakeR2Client:
    """In-memory mock mimicking the subset of boto3 S3 APIs used by download_data."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self._objects = objects

    def get_object(self, Bucket: str, Key: str) -> dict[str, io.BytesIO]:  # noqa: N803
        if Key not in self._objects:
            raise KeyError(f"NoSuchKey: s3://{Bucket}/{Key}")
        return {"Body": io.BytesIO(self._objects[Key])}

    def list_objects_v2(self, Bucket: str, Prefix: str) -> dict[str, list[dict[str, str]]]:  # noqa: N803
        contents = [{"Key": key} for key in sorted(self._objects) if key.startswith(Prefix)]
        return {"Contents": contents}

    def download_file(self, Bucket: str, Key: str, Filename: str) -> None:  # noqa: N803
        if Key not in self._objects:
            raise KeyError(f"NoSuchKey: s3://{Bucket}/{Key}")
        Path(Filename).write_bytes(self._objects[Key])


def _build_mock_landing_zip(zip_path: Path) -> bytes:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for ds in REQUIRED_DATASETS:
            zf.writestr(f"landing/{ds}/part-000.csv", f"dataset\n{ds}\n")
    return zip_path.read_bytes()


def test_load_dotenv_if_present_respects_existing_env(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# Comment line",
                'export R2_ACCOUNT_ID="acct-from-file"',
                "R2_BUCKET_NAME=bucket-from-file",
                "R2_PREFIX='course-2026'",
                "R2_STUDENT_ACCESS_KEY_ID=key-123",
                "R2_STUDENT_SECRET_ACCESS_KEY=secret-456",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.delenv("R2_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("R2_BUCKET_NAME", raising=False)
    monkeypatch.setenv("R2_PREFIX", "explicit-override")

    load_dotenv_if_present(env_file)

    assert os.environ["R2_ACCOUNT_ID"] == "acct-from-file"
    assert os.environ["R2_BUCKET_NAME"] == "bucket-from-file"
    assert os.environ["R2_PREFIX"] == "explicit-override"


def test_parse_sha256_manifest_handles_formats() -> None:
    h1 = "a" * 64
    h2 = "B" * 64
    manifest = f"""
# SHA-256 manifest
{h1}  corptech_200d.zip
{h2} *./nested\\extra_patch.zip
"""
    entries = parse_sha256_manifest(manifest)
    assert entries == [
        (h1, "corptech_200d.zip"),
        (h2.lower(), "nested/extra_patch.zip"),
    ]

    with pytest.raises(ValueError, match="empty"):
        parse_sha256_manifest("\n# only comments\n")

    with pytest.raises(ValueError, match="Invalid manifest"):
        parse_sha256_manifest("not-a-hash  file.zip\n")


def test_download_from_r2_manifest_driven_zip_extraction(tmp_path: Path) -> None:
    zip_path = tmp_path / "corptech_200d.zip"
    zip_bytes = _build_mock_landing_zip(zip_path)
    zip_sha = compute_sha256(zip_path)

    fake_client = _FakeR2Client(
        {
            "course-2026/MANIFEST.sha256": f"{zip_sha}  corptech_200d.zip\n".encode("utf-8"),
            "course-2026/corptech_200d.zip": zip_bytes,
        }
    )

    dest = tmp_path / "data" / "landing"
    download_from_r2(
        account_id="acct-id",
        bucket="course-bucket",
        prefix="course-2026",
        access_key="ak",
        secret_key="sk",
        dest=dest,
        s3_client=fake_client,
    )
    verify_landing_folders(dest)

    for ds in REQUIRED_DATASETS:
        assert (dest / ds / "part-000.csv").is_file()


def test_download_from_r2_discovers_alternate_sha256_manifest(tmp_path: Path) -> None:
    zip_path = tmp_path / "corptech_200d.zip"
    zip_bytes = _build_mock_landing_zip(zip_path)
    zip_sha = compute_sha256(zip_path)

    fake_client = _FakeR2Client(
        {
            "course-2026/corptech_manifest.sha256": (
                f"{zip_sha}  course-2026/corptech_200d.zip\n".encode("utf-8")
            ),
            "course-2026/corptech_200d.zip": zip_bytes,
        }
    )

    dest = tmp_path / "landing"
    download_from_r2(
        account_id="acct-id",
        bucket="course-bucket",
        prefix="course-2026",
        access_key="ak",
        secret_key="sk",
        dest=dest,
        s3_client=fake_client,
    )
    verify_landing_folders(dest)


def test_download_from_r2_fails_on_checksum_mismatch(tmp_path: Path) -> None:
    zip_path = tmp_path / "corptech_200d.zip"
    zip_bytes = _build_mock_landing_zip(zip_path)
    wrong_sha = "0" * 64

    fake_client = _FakeR2Client(
        {
            "course-2026/MANIFEST.sha256": f"{wrong_sha}  corptech_200d.zip\n".encode("utf-8"),
            "course-2026/corptech_200d.zip": zip_bytes,
        }
    )

    dest = tmp_path / "landing"
    with pytest.raises(ValueError, match="checksum mismatch"):
        download_from_r2(
            account_id="acct-id",
            bucket="course-bucket",
            prefix="course-2026",
            access_key="ak",
            secret_key="sk",
            dest=dest,
            s3_client=fake_client,
        )

    assert not (dest / "clients").exists()


def test_verify_landing_folders_fails_when_dataset_missing(tmp_path: Path) -> None:
    dest = tmp_path / "landing"
    for ds in REQUIRED_DATASETS[:-1]:
        ds_dir = dest / ds
        ds_dir.mkdir(parents=True, exist_ok=True)
        (ds_dir / "part-000.csv").write_text("x\n1\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match="validation failed"):
        verify_landing_folders(dest)
