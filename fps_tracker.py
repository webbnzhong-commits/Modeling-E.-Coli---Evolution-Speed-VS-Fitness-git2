import time
from datetime import datetime, timezone, timedelta
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None
from pathlib import Path

_DOT_ALPHA = 110
_DOT_RADIUS = 3
_LA_TZ_NAME = "America/Los_Angeles"
if ZoneInfo is not None:
    try:
        _LA_TZ = ZoneInfo(_LA_TZ_NAME)
    except Exception:
        _LA_TZ = timezone(timedelta(hours=-8), "PST")
else:
    _LA_TZ = timezone(timedelta(hours=-8), "PST")


class FPSTracker:
    def __init__(
        self,
        sample_interval: int = 1000,
        graph_max_seconds: float = 2.0,
        log_path=None,
    ) -> None:
        self.sample_interval = max(1, int(sample_interval))
        self.graph_max_seconds = max(0.1, float(graph_max_seconds))
        self.last_start = time.perf_counter()
        self.last_interval_time = None
        self.iter_times = []
        self.iter_timestamps = []
        self.log_path = Path(log_path) if log_path else None
        self._log_ready = False

    def _ensure_log(self) -> None:
        if not self.log_path or self._log_ready:
            return
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.log_path.exists() or self.log_path.stat().st_size == 0:
            with open(self.log_path, "w") as handle:
                handle.write("timestamp,interval_seconds\n")
        self._log_ready = True

    def _append_log(self) -> None:
        if not self.log_path:
            return
        self._ensure_log()
        try:
            with open(self.log_path, "a") as handle:
                handle.write(f"{time.time():.6f},{self.last_interval_time:.6f}\n")
        except Exception:
            pass

    def update(self, frame_count: int, display_mode: int) -> None:
        if frame_count % self.sample_interval != 0:
            return
        now = time.perf_counter()
        self.last_interval_time = now - self.last_start
        if display_mode >= 1:
            self.iter_times.append(self.last_interval_time)
            self.iter_timestamps.append(time.time())
        self.last_start = now
        self._append_log()

    def fps_estimate(self):
        if self.last_interval_time is None or self.last_interval_time <= 0:
            return None
        return self.sample_interval / self.last_interval_time

    @staticmethod
    def _format_time(ts: float) -> str:
        label = datetime.fromtimestamp(float(ts), tz=_LA_TZ).strftime("%I:%M:%S %p")
        return label.lstrip("0").lower()

    def draw_graph(self, surface, font, x: int, y: int, w: int, h: int) -> None:
        import pygame
        pygame.draw.rect(surface, (80, 80, 80), (x, y, w, h), 1)
        label_top = font.render(f"{self.graph_max_seconds:.1f}s", True, (200, 200, 200))
        label_bot = font.render("0.0s", True, (200, 200, 200))
        surface.blit(label_top, (x + w + 5, y - 5))
        surface.blit(label_bot, (x + w + 5, y + h - 15))
        if len(self.iter_times) > 0:
            mean_1000 = sum(self.iter_times) / len(self.iter_times)
            mean_text = font.render(
                f"-Mean: {mean_1000:.2f}s", True, (200, 200, 200)
            )
            surface.blit(
                mean_text,
                (x + w + 5, y - mean_1000 + h - mean_text.get_height()),
            )
            max_points = w
            count = min(len(self.iter_times), len(self.iter_timestamps), max_points)
            recent = self.iter_times[-count:]
            recent_ts = self.iter_timestamps[-count:]
            overlay = pygame.Surface((w, h), pygame.SRCALPHA)
            dot_color = (0, 200, 255, _DOT_ALPHA)
            for i, tval in enumerate(recent):
                t_clamped = max(0.0, min(self.graph_max_seconds, tval))
                px = i
                py = h - int((t_clamped / self.graph_max_seconds) * h)
                pygame.draw.circle(overlay, dot_color, (px, py), _DOT_RADIUS)
            surface.blit(overlay, (x, y))
            if recent_ts:
                start_label = font.render(
                    self._format_time(recent_ts[0]), True, (200, 200, 200)
                )
                end_label = font.render(
                    self._format_time(recent_ts[-1]), True, (200, 200, 200)
                )
                surface.blit(start_label, (x, y + h + 2))
                surface.blit(
                    end_label, (x + w - end_label.get_width(), y + h + 2)
                )
