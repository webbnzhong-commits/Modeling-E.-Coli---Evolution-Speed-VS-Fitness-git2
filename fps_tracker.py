import time
from pathlib import Path
import pygame


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
        self.last_start = now
        self._append_log()

    def fps_estimate(self):
        if self.last_interval_time is None or self.last_interval_time <= 0:
            return None
        return self.sample_interval / self.last_interval_time

    def draw_graph(self, surface, font, x: int, y: int, w: int, h: int) -> None:
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
            recent = self.iter_times[-max_points:]
            for i, tval in enumerate(recent):
                t_clamped = max(0.0, min(self.graph_max_seconds, tval))
                px = x + i
                py = y + h - int((t_clamped / self.graph_max_seconds) * h)
                pygame.draw.circle(surface, (0, 200, 255), (px, py), 2)
