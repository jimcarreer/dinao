"""Rich-based live dashboard for stress test visualization."""

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
REFRESH_PER_SECOND = 8
VISIBLE_ERRORS = 10

ROLE_STYLES = {
    "Inserter": "green",
    "Transferrer": "cyan",
    "Reader": "blue",
    "Checker": "magenta",
    "Deleter": "red",
}

SPINNER_FRAMES = ["\u25d0", "\u25d3", "\u25d1", "\u25d2"]


@dataclass
class ErrorEvent:
    """A timestamped error for the live error log."""

    timestamp: str
    expected: bool
    category: str
    message: str


class LiveMetrics:
    """Thread-safe metrics store written to by workers, read by the dashboard."""

    def __init__(self, role_names, workers_per_role, duration):
        """Construct live metrics.

        :param role_names: ordered list of worker role names
        :param workers_per_role: number of workers spawned per role
        :param duration: planned test duration in seconds
        """
        self._lock = threading.Lock()
        self.role_names = list(role_names)
        self.workers_per_role = workers_per_role
        self.duration = duration
        self.db_path = ""
        self.start_time = None
        self.finished = False
        self.final_accounts = 0
        self._ops = {r: 0 for r in role_names}
        self._done = {r: 0 for r in role_names}
        self._error_log = deque(maxlen=MAX_ERROR_LOG_SIZE)
        self._expected_total = 0
        self._unexpected_total = 0

    def start(self):
        """Mark the start of the test."""
        self.start_time = time.monotonic()

    @property
    def elapsed(self):
        """Seconds elapsed since start."""
        if self.start_time is None:
            return 0.0
        return time.monotonic() - self.start_time

    def record_op(self, role):
        """Increment the successful-ops counter for a worker role.

        :param role: the role name whose counter to increment
        """
        with self._lock:
            self._ops[role] += 1

    def record_error(self, expected, category, message):
        """Append an error event to the live log.

        :param expected: True if the error matched a registered pattern
        :param category: error category label
        :param message: the error message text
        """
        ts = time.strftime("%H:%M:%S")
        evt = ErrorEvent(ts, expected, category, message[:120])
        with self._lock:
            self._error_log.append(evt)
            if expected:
                self._expected_total += 1
            else:
                self._unexpected_total += 1

    def mark_done(self, role):
        """Note that one worker of the given role has completed.

        :param role: the role name whose done-counter to increment
        """
        with self._lock:
            self._done[role] += 1

    def snapshot(self):
        """Return a consistent point-in-time snapshot of all metrics.

        :returns: dict with all current values
        """
        with self._lock:
            return {
                "elapsed": self.elapsed,
                "duration": self.duration,
                "db_path": self.db_path,
                "ops": dict(self._ops),
                "done": dict(self._done),
                "total_ops": sum(self._ops.values()),
                "errors": list(self._error_log),
                "expected_total": self._expected_total,
                "unexpected_total": self._unexpected_total,
                "finished": self.finished,
            }


class Dashboard:
    """Rich live-updating console dashboard for stress tests."""

    def __init__(self, title, metrics):
        """Construct a dashboard.

        :param title: display title shown in the header
        :param metrics: a LiveMetrics instance to visualize
        """
        self.title = title
        self.metrics = metrics
        self.console = Console()
        self._thread = None

    # -- rendering helpers ------------------------------------------------

    @staticmethod
    def _progress_bar(elapsed, duration, width=48):
        """Build a text-based progress bar string and percentage.

        :param elapsed: seconds elapsed
        :param duration: total duration in seconds
        :param width: character width of the bar
        :returns: (bar_string, fraction_complete)
        """
        pct = min(elapsed / duration, 1.0) if duration > 0 else 0
        filled = int(width * pct)
        if filled < width:
            bar = "\u2501" * filled + "\u257a" + "\u2500" * max(0, width - filled - 1)
        else:
            bar = "\u2501" * width
        return bar, pct

    def _build_header(self, snap):
        """Build the header panel with progress and summary stats.

        :param snap: metrics snapshot dict
        :returns: a Rich Panel
        """
        elapsed = snap["elapsed"]
        duration = snap["duration"]
        bar_str, pct = self._progress_bar(elapsed, duration)
        total_workers = len(self.metrics.role_names) * self.metrics.workers_per_role
        ops_s = snap["total_ops"] / max(elapsed, 0.1)

        txt = Text()
        txt.append("  DB      ", style="dim")
        txt.append(snap["db_path"] or "...", style="cyan")
        txt.append("\n")
        txt.append("  Workers ", style="dim")
        txt.append(str(self.metrics.workers_per_role), style="bold")
        txt.append(" per role ", style="dim")
        txt.append(f"({total_workers} total)", style="bold cyan")
        txt.append("  |  Ops ", style="dim")
        txt.append(f"{snap['total_ops']:,}", style="bold green")
        txt.append(f"  ({ops_s:,.0f}/s)", style="dim green")
        txt.append("\n\n  ")

        bar_color = "green" if pct < 0.7 else ("yellow" if pct < 0.9 else "red")
        txt.append(bar_str, style=f"bold {bar_color}")
        txt.append(f"  {elapsed:5.1f}s / {duration:.0f}s ", style="bold")
        if snap["finished"]:
            txt.append(" COMPLETE ", style="bold white on green")

        return Panel(
            txt,
            title=f"[bold bright_blue] {self.title} [/]",
            border_style="bright_blue",
        )

    def _build_worker_table(self, snap):
        """Build the per-role worker activity table.

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
        table.add_column("Role", style="bold", min_width=14)
        table.add_column("Status", justify="center", min_width=10)
        table.add_column("Ops", justify="right", min_width=8)
        table.add_column("Ops/s", justify="right", min_width=8)
        table.add_column("Throughput", min_width=20)

        elapsed = max(snap["elapsed"], 0.1)
        max_ops = max(snap["ops"].values(), default=1) or 1

        for role in self.metrics.role_names:
            ops = snap["ops"].get(role, 0)
            done = snap["done"].get(role, 0)
            total = self.metrics.workers_per_role
            ops_s = ops / elapsed
            color = ROLE_STYLES.get(role, "white")

            if done >= total:
                status = Text("  Done", style="dim green")
            else:
                idx = int(snap["elapsed"] * 4) % len(SPINNER_FRAMES)
                active = total - done
                status = Text(
                    f"{SPINNER_FRAMES[idx]} {active}/{total}",
                    style=f"bold {color}",
                )

            bar_w = 20
            filled = int(bar_w * ops / max_ops) if max_ops else 0
            bar = Text()
            bar.append("\u2588" * filled, style=color)
            bar.append("\u2591" * (bar_w - filled), style="dim")

            table.add_row(
                Text(role, style=color),
                status,
                f"{ops:,}",
                f"{ops_s:,.1f}",
                bar,
            )

        table.add_section()
        total_ops = snap["total_ops"]
        total_s = total_ops / elapsed
        table.add_row(
            Text("TOTAL", style="bold"),
            Text(""),
            Text(f"{total_ops:,}", style="bold"),
            Text(f"{total_s:,.1f}", style="bold"),
            Text(""),
        )
        return table

    def _build_error_panel(self, snap):
        """Build the scrolling error log panel.

        :param snap: metrics snapshot dict
        :returns: a Rich Panel
        """
        exp = snap["expected_total"]
        unexp = snap["unexpected_total"]

        title = Text(" Errors  ")
        title.append(f"expected {exp}", style="yellow")
        title.append("  ", style="dim")
        unexp_style = "bold red" if unexp > 0 else "green"
        title.append(f"unexpected {unexp}", style=unexp_style)
        title.append(" ")

        errors = snap["errors"]
        visible = errors[-VISIBLE_ERRORS:]

        txt = Text()
        if not visible:
            txt.append("  (waiting for errors...)", style="dim italic")
        else:
            for i, evt in enumerate(visible):
                if i > 0:
                    txt.append("\n")
                txt.append(f"  {evt.timestamp} ", style="dim")
                if evt.expected:
                    txt.append(">> ", style="yellow")
                    txt.append(f"{evt.category:<20s} ", style="yellow")
                else:
                    txt.append("!! ", style="bold red")
                    txt.append(f"{evt.category:<20s} ", style="bold red")
                txt.append(evt.message, style="dim")

        border = "red" if unexp > 0 else "bright_blue"
        return Panel(txt, title=title, border_style=border)

    def _render(self):
        """Compose the full dashboard from header, table, and error log.

        :returns: a Rich Group renderable
        """
        snap = self.metrics.snapshot()
        return Group(
            self._build_header(snap),
            self._build_worker_table(snap),
            self._build_error_panel(snap),
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

    def print_summary(self, result):
        """Print a static final summary panel after the live display ends.

        :param result: a StressResult (or compatible) object
        """
        self.console.print()
        rate = result.total_ops / max(result.elapsed, 0.1)

        txt = Text()
        txt.append("  Duration      ", style="dim")
        txt.append(f"{result.elapsed:.1f}s\n", style="bold")
        txt.append("  Operations    ", style="dim")
        txt.append(f"{result.total_ops:,}\n", style="bold green")
        txt.append("  Throughput    ", style="dim")
        txt.append(f"{rate:,.1f} ops/s\n", style="bold cyan")
        txt.append("  Accounts      ", style="dim")
        txt.append(f"{result.final_accounts:,} remaining\n", style="bold")

        exp = result.errors.expected_count
        unexp = result.errors.unexpected_count

        txt.append("\n  Expected      ", style="dim")
        style = "yellow" if exp else "green"
        txt.append(f"{exp}\n", style=style)
        for name, cat in sorted(result.errors.expected.items()):
            txt.append(f"    {name}: ", style="yellow")
            txt.append(f"{cat.count}\n", style="bold yellow")

        txt.append("  Unexpected    ", style="dim")
        if unexp:
            txt.append(f"{unexp}\n", style="bold red")
            for name, cat in sorted(result.errors.unexpected.items()):
                txt.append(f"    {name}: ", style="red")
                txt.append(f"{cat.count}\n", style="bold red")
                for s in cat.samples[:3]:
                    txt.append(f"      [{s.exc_type}] {s.message}\n", style="dim red")
        else:
            txt.append(f"{unexp}\n", style="bold green")

        verdict = "PASS" if unexp == 0 else "FAIL"
        v_style = "bold green" if unexp == 0 else "bold red"
        self.console.print(Panel(txt, title=f"[{v_style}] {verdict} [/]", border_style=v_style))
