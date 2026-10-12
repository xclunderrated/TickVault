"""Protect internal vault and provider files from export publication."""

from pathlib import Path

from .config import get_config


def validate_export_destination(path: str | Path) -> Path:
    """Validate before creating a lock, temporary file or destination directory."""
    from ..provider._state import state_path

    destination = Path(path)
    resolved = destination.resolve()
    settings = get_config()
    databases = {
        settings.metadata_db_path.resolve(): "metadata.db",
        state_path().resolve(): "provider state",
        state_path(settings.provider_state_directory).resolve(): "provider state",
    }
    for database, label in databases.items():
        if resolved in {
            database,
            *(Path(str(database) + suffix) for suffix in ("-wal", "-shm", "-journal")),
        }:
            raise ValueError(f"tick exports must use a separate file from {label}")
        if label == "provider state":
            raw = database.parent / (database.stem + "-raw")
            if resolved.is_relative_to(raw.resolve()):
                raise ValueError(
                    "tick exports must stay outside the provider raw cache"
                )
    if resolved == settings.log_file_path.resolve():
        raise ValueError("tick exports must use a separate file from the active log")
    if resolved.is_relative_to(settings.save_directory.resolve()):
        raise ValueError("tick exports must stay outside the raw download cache")
    return destination
