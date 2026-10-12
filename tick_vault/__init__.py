"""TickVault: High-performance financial tick data downloader and reader."""

from .api import download_and_read
from .config import reload_config
from .download import DownloadProgress, DownloadReport
from .download.jobs import (
    get_download_job,
    resume_download_job,
    run_download_job,
    submit_download_job,
)
from .download.probe import FeedProbe, probe_feed
from .downloader import download_range
from .provider import ManagedJob, ProviderCoordinator, ProviderPolicy
from .read.storage import (
    export_hdf5,
    export_sqlite,
    iter_hdf5,
    iter_sqlite,
    read_hdf5,
    read_sqlite,
)
from .reader import iter_tick_data, read_tick_data

__version__ = "0.3.0"

__all__ = [
    "reload_config",
    "download_range",
    "read_tick_data",
    "iter_tick_data",
    "download_and_read",
    "export_sqlite",
    "iter_sqlite",
    "export_hdf5",
    "iter_hdf5",
    "read_sqlite",
    "read_hdf5",
    "DownloadProgress",
    "DownloadReport",
    "submit_download_job",
    "resume_download_job",
    "run_download_job",
    "get_download_job",
    "ManagedJob",
    "ProviderCoordinator",
    "ProviderPolicy",
    "probe_feed",
    "FeedProbe",
]
