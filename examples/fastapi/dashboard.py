"""Rich-based live dashboard for FastAPI stress test visualization."""

import statistics
import threading
import time
from collections import deque
from dataclasses import dataclass

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

MAX_ERROR_LOG_SIZE = 100
RECENT_WINDOW = 1000
REFRESH_PER_SECOND = 8
VISIBLE_ERRORS = 10

ACTION_STYLES = {
    "POST /items": "green",
    "GET /items": "cyan",
    "GET /summed": "magenta",
}

SPINNER_FRAMES = ["\u25d0", "\u25d3", "\u25d1", "\u25d2"]


@dataclass
class ErrorEvent:
    """A timestamped non-2xx response for the live error log."""

    timestamp: str
    action: str
    status_code: int
    message: str


class LiveMetrics:
    """Thread-safe metrics store written to by workers, read by the dashboard.

    Workers call ``record()`` on the hot path.  The dashboard calls
    ``snapshot()`` at ~8 Hz from its own thread; all derived math
    (mean, median) is computed outside the lock on copied data so
    workers are never blocked by rendering.
    """

    def __init__(self, action_names, total_workers, target_url):
        """Construct live metrics.

        :param action_names: ordered list of action labels
        :param total_workers: total number of worker threads
        :param target_url: the base URL being tested
        """
        self._lock = threading.Lock()
        self.action_names = list(action_names)
        self.total_workers = total_workers
        self.target_url = target_url
        self.start_time = None
        self.finished = False

        self._ops = {a: 0 for a in action_names}
        self._time_sum = {a: 0.0 for a in action_names}
        self._time_min = {a: float("inf") for a in action_names}
        self._time_max = {a: 0.0 for a in action_names}
        self._recent_times = {a: deque(maxlen=RECENT_WINDOW) for a in action_names}
        self._status_codes = {}
        self._status_by_action = {a: {} for a in action_names}
        self._error_log = deque(maxlen=MAX_ERROR_LOG_SIZE)
        self._active_workers = total_workers

    def start(self):
        """Mark the start of the test."""
        self.start_time = time.monotonic()

    @property
    def elapsed(self):
        """Seconds elapsed since start."""
        if self.start_time is None:
            return 0.0
        return time.monotonic() - self.start_time

    def record(self, action, response_time, status_code):
        """Record one completed request.

        This is the hot path -- all work is O(1) under the lock.

        :param action: action label (e.g. ``"POST /items"``)
        :param response_time: elapsed seconds for the request
        :param status_code: HTTP status code returned
        """
        with self._lock:
            self._ops[action] += 1
            self._time_sum[action] += response_time
            if response_time < self._time_min[action]:
                self._time_min[action] = response_time
            if response_time > self._time_max[action]:
                self._time_max[action] = response_time
            self._recent_times[action].append(response_time)

            self._status_codes[status_code] = self._status_codes.get(status_code, 0) + 1
            action_statuses = self._status_by_action[action]
            action_statuses[status_code] = action_statuses.get(status_code, 0) + 1

            if not (200 <= status_code < 300):
                ts = time.strftime("%H:%M:%S")
                self._error_log.append(
                    ErrorEvent(ts, action, status_code, f"HTTP {status_code}")
                )

    def mark_worker_done(self):
        """Decrement the active worker count.

        Called by each worker thread as it exits.
        """
        with self._lock:
            self._active_workers -= 1

    def snapshot(self):
        """Return a consistent point-in-time copy of all metrics.

        Phase 1 (under lock): copy scalars, dicts, and bounded queues.
        Phase 2 (outside lock): compute derived stats from copies.

        :returns: dict with all current and derived values
        """
        with self._lock:
            elapsed = self.elapsed
            ops = dict(self._ops)
            time_sum = dict(self._time_sum)
            time_min = dict(self._time_min)
            time_max = dict(self._time_max)
            recent = {a: list(d) for a, d in self._recent_times.items()}
            status_codes = dict(self._status_codes)
            status_by_action = {a: dict(d) for a, d in self._status_by_action.items()}
            errors = list(self._error_log)
            active = self._active_workers
            finished = self.finished

        total_ops = sum(ops.values())
        safe_elapsed = max(elapsed, 0.1)

        actions = {}
        for a in self.action_names:
            count = ops[a]
            if count > 0:
                mean = (time_sum[a] / count) * 1000
                median = statistics.median(recent[a]) * 1000 if recent[a] else 0.0
                low = time_min[a] * 1000
                high = time_max[a] * 1000
            else:
                mean = median = low = high = 0.0
            actions[a] = {
                "ops": count,
                "ops_s": count / safe_elapsed,
                "mean_ms": mean,
                "median_ms": median,
                "min_ms": low,
                "max_ms": high,
                "statuses": status_by_action.get(a, {}),
            }

        return {
            "elapsed": elapsed,
            "total_ops": total_ops,
            "ops_s": total_ops / safe_elapsed,
            "active_workers": active,
            "actions": actions,
            "status_codes": status_codes,
            "errors": errors,
            "finished": finished,
        }


class Dashboard:
    """Rich live-updating console dashboard for the FastAPI tester."""

    def __init__(self, metrics):
        """Construct a dashboard.

        :param metrics: a ``LiveMetrics`` instance to visualize
        """
        self.metrics = metrics
        self.console = Console()
        self._thread = None

    # -- rendering helpers ------------------------------------------------

    def _build_header(self, snap):
        """Build the header panel with target URL, workers, and throughput.

        :param snap: metrics snapshot dict
        :returns: a Rich Panel
        """
        elapsed = snap["elapsed"]
        active = snap["active_workers"]
        total = self.metrics.total_workers
        idx = int(elapsed * 4) % len(SPINNER_FRAMES)
        spinner = SPINNER_FRAMES[idx]

        txt = Text()
        txt.append("  Target  ", style="dim")
        txt.append(self.metrics.target_url, style="cyan")
        txt.append("\n")
        txt.append("  Workers ", style="dim")
        txt.append(f"{active}/{total}", style="bold")
        txt.append(" active", style="dim")
        txt.append("  |  Ops ", style="dim")
        txt.append(f"{snap['total_ops']:,}", style="bold green")
        txt.append(f"  ({snap['ops_s']:,.0f}/s)", style="dim green")
        txt.append("\n")
        txt.append("  Elapsed ", style="dim")
        txt.append(f"{elapsed:5.1f}s ", style="bold")
        if snap["finished"]:
            txt.append(" DONE ", style="bold white on green")
        else:
            txt.append(f" {spinner}", style="bold bright_blue")

        return Panel(
            txt,
            title="[bold bright_blue] FastAPI Stress Test [/]",
            border_style="bright_blue",
        )

    def _build_action_table(self, snap):
        """Build the per-action table with timing and throughput stats.

        :param snap: metrics snapshot dict
        :returns: a Rich Table
        """
        table = Table(
            show_header=True,
            header_style="bold",
            border_style="bright_blue",
            expand=True,
            padding=(0, 1),
        )
        table.add_column("Action", style="bold", min_width=14)
        table.add_column("Status", justify="center", min_width=8)
        table.add_column("Ops", justify="right", min_width=8)
        table.add_column("Ops/s", justify="right", min_width=8)
        table.add_column("Mean ms", justify="right", min_width=9)
        table.add_column("Median ms", justify="right", min_width=9)
        table.add_column("Min ms", justify="right", min_width=9)
        table.add_column("Max ms", justify="right", min_width=9)
        table.add_column("Throughput", min_width=16)

        elapsed = snap["elapsed"]
        actions = snap["actions"]
        max_ops = max((a["ops"] for a in actions.values()), default=1) or 1

        for action_name in self.metrics.action_names:
            a = actions[action_name]
            color = ACTION_STYLES.get(action_name, "white")

            if snap["finished"]:
                status = Text("  Done", style="dim green")
            elif a["ops"] == 0:
                status = Text("  Wait", style="dim")
            else:
                idx = int(elapsed * 4) % len(SPINNER_FRAMES)
                status = Text(f"{SPINNER_FRAMES[idx]} Live", style=f"bold {color}")

            bar_w = 16
            filled = int(bar_w * a["ops"] / max_ops) if max_ops else 0
            bar = Text()
            bar.append("\u2588" * filled, style=color)
            bar.append("\u2591" * (bar_w - filled), style="dim")

            table.add_row(
                Text(action_name, style=color),
                status,
                f"{a['ops']:,}",
                f"{a['ops_s']:,.1f}",
                f"{a['mean_ms']:.2f}",
                f"{a['median_ms']:.2f}",
                f"{a['min_ms']:.2f}",
                f"{a['max_ms']:.2f}",
                bar,
            )

        table.add_section()
        table.add_row(
            Text("TOTAL", style="bold"),
            Text(""),
            Text(f"{snap['total_ops']:,}", style="bold"),
            Text(f"{snap['ops_s']:,.1f}", style="bold"),
            Text(""), Text(""), Text(""), Text(""),
            Text(""),
        )
        return table

    def _build_status_panel(self, snap):
        """Build the status code breakdown and scrolling error log.

        :param snap: metrics snapshot dict
        :returns: a Rich Panel
        """
        status_codes = snap["status_codes"]
        total = sum(status_codes.values()) or 1
        success = sum(cnt for code, cnt in status_codes.items() if 200 <= code < 300)
        errors_count = total - success

        title = Text(" Status  ")
        title.append(f"2xx: {success}", style="green")
        title.append("  ", style="dim")
        err_style = "bold red" if errors_count > 0 else "green"
        title.append(f"errors: {errors_count}", style=err_style)
        title.append(" ")

        txt = Text()
        if status_codes:
            txt.append("  Codes: ", style="dim")
            for i, code in enumerate(sorted(status_codes.keys())):
                if i > 0:
                    txt.append("  ", style="dim")
                cnt = status_codes[code]
                pct = (cnt / total) * 100
                code_style = "green" if 200 <= code < 300 else "red"
                txt.append(f"{code}", style=f"bold {code_style}")
                txt.append(f"={cnt} ({pct:.1f}%)", style="dim")
            txt.append("\n")

        errors = snap["errors"]
        visible = errors[-VISIBLE_ERRORS:]
        if visible:
            txt.append("\n")
            for i, evt in enumerate(visible):
                if i > 0:
                    txt.append("\n")
                txt.append(f"  {evt.timestamp} ", style="dim")
                txt.append(f"{evt.action:<14s} ", style="yellow")
                txt.append(f"{evt.status_code} ", style="bold red")
                txt.append(evt.message, style="dim")
        elif not status_codes:
            txt.append("  (waiting for responses...)", style="dim italic")

        border = "red" if errors_count > 0 else "bright_blue"
        return Panel(txt, title=title, border_style=border)

    def _render(self):
        """Compose the full dashboard from header, table, and status panel.

        :returns: a Rich Group renderable
        """
        snap = self.metrics.snapshot()
        return Group(
            self._build_header(snap),
            self._build_action_table(snap),
            self._build_status_panel(snap),
        )

    # -- lifecycle --------------------------------------------------------

    def _loop(self):
        """Run the Rich Live display loop until metrics.finished is set."""
        interval = 1.0 / REFRESH_PER_SECOND
        with Live(
            self._render(),
            console=self.console,
            refresh_per_second=REFRESH_PER_SECOND,
            transient=True,
        ) as live:
            while not self.metrics.finished:
                live.update(self._render())
                time.sleep(interval)
            live.update(self._render())

    def start(self):
        """Start the dashboard in a background daemon thread."""
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Signal the dashboard to stop and wait for the thread to exit."""
        self.metrics.finished = True
        if self._thread is not None:
            self._thread.join(timeout=3)

    def print_summary(self, snap):
        """Print a static final summary panel after the live display ends.

        :param snap: a snapshot dict from ``LiveMetrics.snapshot()``
        """
        self.console.print()

        txt = Text()
        txt.append("  Duration      ", style="dim")
        txt.append(f"{snap['elapsed']:.1f}s\n", style="bold")
        txt.append("  Operations    ", style="dim")
        txt.append(f"{snap['total_ops']:,}\n", style="bold green")
        txt.append("  Throughput    ", style="dim")
        txt.append(f"{snap['ops_s']:,.1f} ops/s\n", style="bold cyan")

        status_codes = snap["status_codes"]
        total = sum(status_codes.values()) or 1
        success = sum(cnt for code, cnt in status_codes.items() if 200 <= code < 300)
        errors_count = total - success

        txt.append("\n  Status Codes\n", style="dim")
        for code in sorted(status_codes.keys()):
            cnt = status_codes[code]
            pct = (cnt / total) * 100
            code_style = "green" if 200 <= code < 300 else "red"
            txt.append(f"    {code}: ", style=f"bold {code_style}")
            txt.append(f"{cnt:,} ({pct:.1f}%)\n", style="dim")

        txt.append("\n  Per Action\n", style="dim")
        for action_name in self.metrics.action_names:
            a = snap["actions"][action_name]
            color = ACTION_STYLES.get(action_name, "white")
            txt.append(f"    {action_name:<14s}", style=color)
            txt.append(f"  ops={a['ops']:,}", style="dim")
            txt.append(f"  mean={a['mean_ms']:.2f}ms", style="dim")
            txt.append(f"  median={a['median_ms']:.2f}ms", style="dim")
            txt.append(f"  min={a['min_ms']:.2f}ms", style="dim")
            txt.append(f"  max={a['max_ms']:.2f}ms\n", style="dim")

        verdict = "PASS" if errors_count == 0 else "DONE"
        v_style = "bold green" if errors_count == 0 else "bold yellow"
        self.console.print(
            Panel(txt, title=f"[{v_style}] {verdict} [/]", border_style=v_style)
        )
