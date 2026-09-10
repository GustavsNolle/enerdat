"""Resources: the two upstream APIs and the immutable landing zone."""

from __future__ import annotations

import time
from pathlib import Path

import dagster as dg
import pandas as pd
import requests


class EntsoeResource(dg.ConfigurableResource):
    """Thin wrapper over entsoe-py's pandas client.

    Exists mainly so assets can be tested without network access, and so the
    retry policy lives in one place. The Transparency Platform is not a
    high-availability service -- it returns 5xx and, during outages, blanket
    404s -- so every call retries with backoff.
    """

    api_key: str
    max_attempts: int = 4
    backoff_seconds: float = 2.0

    # Override the API host without touching entsoe-py. ENTSO-E has moved this
    # endpoint before -- on 2026-09-08 the legacy web-api host began returning
    # 404 for every route, including unauthenticated ones -- and the client
    # library lagged the change. Set ENTSOE_ENDPOINT_URL to repoint.
    endpoint_url: str = ""

    # entsoe-py defaults to timeout=None, i.e. block forever. That is not
    # theoretical: a year-range request to the Transparency Platform was
    # observed sitting in CLOSE-WAIT with unread bytes for 18+ minutes after
    # the server had hung up, and no retry wrapper can help because control
    # never comes back. Generous enough for a legitimate multi-month range,
    # finite either way.
    request_timeout_seconds: int = 180

    def client(self):
        import entsoe.entsoe as entsoe_module
        from entsoe import EntsoePandasClient

        if self.endpoint_url:
            # Read per-request from the module global, so patching it here
            # takes effect for every call this client makes.
            entsoe_module.URL = self.endpoint_url

        return EntsoePandasClient(
            api_key=self.api_key,
            timeout=self.request_timeout_seconds,
            # Retries are handled in fetch(). Leaving entsoe-py's own default
            # of 3 with a 10s delay would multiply out to a dozen attempts
            # before a genuine failure ever surfaced.
            retry_count=1,
            retry_delay=0,
        )

    def fetch(self, method: str, *args, **kwargs):
        """Call `method` on the client, retrying transient failures.

        Returns None when the platform answers "no matching data" -- that is a
        publication fact about the zone, not an error, and callers treat it as
        an empty partition rather than a failure.
        """
        from entsoe.exceptions import NoMatchingDataError

        import requests

        last: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                return getattr(self.client(), method)(*args, **kwargs)
            except NoMatchingDataError:
                return None
            except requests.HTTPError as exc:
                status = getattr(exc.response, "status_code", None)
                if status in (401, 403):
                    raise RuntimeError(
                        f"{method}: ENTSO-E rejected the API key ({status}). "
                        "Check ENTSOE_API_KEY and that the key has web-API "
                        "access enabled."
                    ) from exc
                if status == 404:
                    # A routing failure, not a transient one: retrying it just
                    # burns four minutes before failing anyway. 404 on an
                    # unauthenticated route means the endpoint moved.
                    current = self.endpoint_url or "entsoe-py default"
                    raise RuntimeError(
                        f"{method}: ENTSO-E returned 404 for the API route "
                        f"({current}). The endpoint has moved -- set "
                        "ENTSOE_ENDPOINT_URL to the current host."
                    ) from exc
                last = exc
                if attempt < self.max_attempts:
                    time.sleep(self.backoff_seconds * attempt)
            except Exception as exc:  # noqa: BLE001 - retry everything else
                last = exc
                if attempt < self.max_attempts:
                    time.sleep(self.backoff_seconds * attempt)
        raise RuntimeError(
            f"{method} failed after {self.max_attempts} attempts: {last}"
        ) from last


    def fetch_range(self, method: str, *args, start, end, **kwargs):
        """Like fetch(), but split into MAX_QUERY_DAYS chunks and concatenated.

        Wide ranges are where this API misbehaves, so a backfill asks for
        several bounded windows rather than one open-ended one. Chunks that
        report no data are skipped rather than failing the range: a zone can
        legitimately start publishing part-way through the window.
        """
        import pandas as pd

        from enerdat.config import MAX_QUERY_DAYS

        edges = list(pd.date_range(start, end, freq=f"{MAX_QUERY_DAYS}D"))
        if not edges or edges[-1] < end:
            edges.append(end)

        pieces = []
        for chunk_start, chunk_end in zip(edges, edges[1:]):
            piece = self.fetch(method, *args, start=chunk_start, end=chunk_end, **kwargs)
            if piece is not None and len(piece):
                pieces.append(piece)

        if not pieces:
            return None

        combined = pd.concat(pieces)
        # Chunk boundaries are shared between adjacent windows, so the same
        # timestamp can arrive twice; keep the later retrieval of each.
        return combined[~combined.index.duplicated(keep="last")].sort_index()


class OpenMeteoResource(dg.ConfigurableResource):
    """Archived *forecasts* from Open-Meteo -- never reanalysis.

    The host matters more than any parameter here:

      historical-forecast-api  archived model runs. What was predicted, when.
      archive-api              ERA5 reanalysis, reconstructed with hindsight.

    Only the first is admissible as a feature source. Using the second would
    leak future information into training and inflate every metric.
    """

    base_url: str = "https://historical-forecast-api.open-meteo.com/v1/forecast"
    model: str = "icon_seamless"
    timeout_seconds: int = 60
    max_attempts: int = 4
    backoff_seconds: float = 2.0

    def fetch_point(
        self,
        lat: float,
        lon: float,
        start_date: str,
        end_date: str,
        variables: list[str],
        lead_days: int,
    ) -> pd.DataFrame:
        """Hourly archived forecast for one grid point, at a fixed lead time.

        `lead_days` selects Open-Meteo's `previous_dayN` variant, i.e. the run
        issued roughly N days before each valid time. Requesting the plain
        variable would return the *latest* available run, which for historical
        dates is a short-lead forecast and would leak.
        """
        suffixed = [f"{v}_previous_day{lead_days}" for v in variables]
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": ",".join(suffixed),
            "models": self.model,
            "timezone": "UTC",
            "windspeed_unit": "ms",
        }

        last: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = requests.get(
                    self.base_url, params=params, timeout=self.timeout_seconds
                )
                response.raise_for_status()
                payload = response.json()
                if "error" in payload:
                    raise RuntimeError(payload.get("reason", "open-meteo error"))
                break
            except Exception as exc:  # noqa: BLE001
                last = exc
                if attempt == self.max_attempts:
                    raise RuntimeError(
                        f"open-meteo failed after {self.max_attempts} attempts: {exc}"
                    ) from exc
                time.sleep(self.backoff_seconds * attempt)

        hourly = payload["hourly"]
        frame = pd.DataFrame(hourly)
        frame["time"] = pd.to_datetime(frame["time"], utc=True)

        # Drop the _previous_dayN suffix so downstream schemas are stable
        # across lead-time changes; the lead time is recorded as a column.
        renames = {f"{v}_previous_day{lead_days}": v for v in variables}
        frame = frame.rename(columns=renames)

        long = frame.melt(
            id_vars="time", var_name="variable", value_name="value"
        ).rename(columns={"time": "valid_time_utc"})
        long["lead_days"] = lead_days
        return long


class LakeResource(dg.ConfigurableResource):
    """Append-only Parquet landing zone.

    Re-materialising a partition never overwrites: each run writes a new file
    stamped with its retrieval time. That is what makes revisions observable --
    ENTSO-E restates "actual" values after publication, and a mart that
    overwrites can never reproduce what was known on a past date.
    """

    root: str

    def partition_dir(self, dataset: str, partition_key: str) -> Path:
        return Path(self.root) / dataset / f"delivery_date={partition_key}"

    def write(
        self,
        dataset: str,
        partition_key: str,
        frame: pd.DataFrame,
        retrieved_at: pd.Timestamp,
    ) -> Path:
        directory = self.partition_dir(dataset, partition_key)
        directory.mkdir(parents=True, exist_ok=True)
        stamp = retrieved_at.strftime("%Y%m%dT%H%M%S%fZ")
        path = directory / f"retrieved_at={stamp}.parquet"
        frame.to_parquet(path, index=False)
        return path

    def glob(self, dataset: str) -> str:
        return str(Path(self.root) / dataset / "**" / "*.parquet")
