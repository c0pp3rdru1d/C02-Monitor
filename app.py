from __future__ import annotations

import csv
import io
import threading
import queue
import traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple

import requests
import tkinter as tk
from tkinter import ttk, messagebox


# --- Data sources (public) ---
NOAA_DAILY_CSV = "https://gml.noaa.gov/webdata/ccgg/trends/co2/co2_daily_mlo.csv"
OWID_CO2_CSV = "https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv"


# --- Constants / assumptions ---
PPM_TO_GTCO2_IN_ATMOSPHERE = 7.80432  # ≈ 2.13 GtC * 44/12 (GtCO2)
PREINDUSTRIAL_PPM = 280.0

BUDGETS_GTCO2 = {
    "1.5°C budget (50% chance) ~580 GtCO₂": 580.0,
    "1.5°C budget (66% chance) ~420 GtCO₂": 420.0,
}


@dataclass(frozen=True)
class Co2Snapshot:
    date: datetime
    ppm: float


def _http_get_text(url: str, timeout: int = 20) -> str:
    r = requests.get(url, timeout=timeout, headers={"User-Agent": "co2-budget-tracker/1.0"})
    r.raise_for_status()
    return r.text


def fetch_latest_noaa_daily_ppm() -> Co2Snapshot:
    """
    Parses NOAA's co2_daily_mlo.csv and returns the most recent valid daily mean (ppm).
    """
    text = _http_get_text(NOAA_DAILY_CSV)
    rows: list[Tuple[datetime, float]] = []

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 5:
            continue

        try:
            year = int(parts[0])
            month = int(parts[1])
            day = int(parts[2])
            avg = float(parts[4])
        except ValueError:
            continue

        if avg <= 0:
            continue

        rows.append((datetime(year, month, day), avg))

    if not rows:
        raise RuntimeError("No valid rows found in NOAA daily CO₂ file.")

    dt, ppm = max(rows, key=lambda x: x[0])
    return Co2Snapshot(date=dt, ppm=ppm)


def fetch_world_emissions_owid(start_year: int, end_year: int) -> list[Tuple[int, float]]:
    """
    Returns (year, emissions_gtco2) for World from OWID.
    OWID column 'co2' is annual CO₂ emissions in million tonnes (MtCO2).
    Convert MtCO2 -> GtCO2 by dividing by 1000.
    """
    text = _http_get_text(OWID_CO2_CSV)
    reader = csv.DictReader(io.StringIO(text))

    out: list[Tuple[int, float]] = []
    for row in reader:
        if row.get("country") != "World":
            continue

        try:
            year = int(row["year"])
        except Exception:
            continue

        if year < start_year or year > end_year:
            continue

        co2_mt = row.get("co2")
        if not co2_mt:
            continue
        try:
            co2_mt_f = float(co2_mt)
        except ValueError:
            continue

        out.append((year, co2_mt_f / 1000.0))  # GtCO2

    out.sort(key=lambda x: x[0])
    return out


def gtco2_in_atmosphere_from_ppm(ppm: float) -> float:
    return ppm * PPM_TO_GTCO2_IN_ATMOSPHERE


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("CO₂ Tracker — Stock + Budget")
        self.geometry("780x420")
        self.minsize(740, 400)

        self.last_snapshot: Optional[Co2Snapshot] = None

        # UI state (Tk vars must only be touched on main thread)
        self.budget_choice = tk.StringVar(value=list(BUDGETS_GTCO2.keys())[0])
        self.budget_start_year = tk.StringVar(value="2020")  # string is safer while typing

        # Thread results queue
        self._q: queue.Queue[tuple] = queue.Queue()
        self._refresh_in_flight = False

        # Header
        header = ttk.Frame(self, padding=12)
        header.pack(fill="x")

        ttk.Label(header, text="CO₂ Tracker", font=("TkDefaultFont", 16, "bold")).pack(side="left")
        self.status = ttk.Label(header, text="Ready")
        self.status.pack(side="right")

        # Controls
        controls = ttk.Frame(self, padding=(12, 0, 12, 12))
        controls.pack(fill="x")

        ttk.Label(controls, text="Budget:").grid(row=0, column=0, sticky="w")
        ttk.Combobox(
            controls,
            textvariable=self.budget_choice,
            values=list(BUDGETS_GTCO2.keys()),
            state="readonly",
            width=38,
        ).grid(row=0, column=1, sticky="w", padx=(8, 18))

        ttk.Label(controls, text="Budget start year:").grid(row=0, column=2, sticky="w")

        self.start_year_box = ttk.Spinbox(
            controls,
            from_=1990,
            to=datetime.now().year,
            textvariable=self.budget_start_year,
            width=8,
        )
        self.start_year_box.grid(row=0, column=3, sticky="w", padx=(8, 0))

        self.refresh_btn = ttk.Button(controls, text="Refresh", command=self.refresh_async)
        self.refresh_btn.grid(row=0, column=4, sticky="e", padx=(18, 0))

        controls.grid_columnconfigure(5, weight=1)

        # Main panel
        main = ttk.Frame(self, padding=12)
        main.pack(fill="both", expand=True)

        self.cards = {}
        titles = [
            "Latest CO₂ (ppm)",
            "CO₂ in atmosphere (GtCO₂)",
            "Above pre-industrial (ppm)",
            "Estimated budget used (GtCO₂)",
            "Estimated budget remaining (GtCO₂)",
        ]
        for i, title in enumerate(titles):
            frame = ttk.Labelframe(main, text=title, padding=12)
            frame.grid(row=i // 2, column=i % 2, sticky="nsew", padx=8, pady=8)
            value = ttk.Label(frame, text="—", font=("TkDefaultFont", 14, "bold"))
            value.pack(anchor="w")
            sub = ttk.Label(frame, text="", foreground="#555555")
            sub.pack(anchor="w", pady=(6, 0))
            self.cards[title] = (value, sub)

        main.grid_columnconfigure(0, weight=1)
        main.grid_columnconfigure(1, weight=1)
        main.grid_rowconfigure(0, weight=1)
        main.grid_rowconfigure(1, weight=1)
        main.grid_rowconfigure(2, weight=1)

        # Footer
        footer = ttk.Frame(self, padding=(12, 0, 12, 12))
        footer.pack(fill="x")
        ttk.Label(
            footer,
            text=(
                "Note: ‘CO₂ in atmosphere’ is derived from ppm using a standard conversion. "
                "Budget math uses annual global emissions (OWID) and is a simplified tracker."
            ),
            foreground="#666666",
        ).pack(anchor="w")

        # Start polling queue (main thread)
        self.after(100, self._poll_queue)

        # initial load
        self.refresh_async()

    def set_status(self, msg: str) -> None:
        self.status.configure(text=msg)

    def _parse_start_year_main_thread(self) -> int:
        """
        Read & validate user input on MAIN THREAD.
        """
        raw = (self.budget_start_year.get() or "").strip()
        try:
            y = int(raw)
        except ValueError:
            y = 2020

        nowy = datetime.now().year
        if y < 1990:
            y = 1990
        if y > nowy:
            y = nowy
        return y

    def refresh_async(self) -> None:
        """
        Kick off a refresh. MUST be called from main thread (button callback is).
        """
        if self._refresh_in_flight:
            return

        self._refresh_in_flight = True
        self.refresh_btn.configure(state="disabled")

        # Read Tk variables ONLY on main thread
        start_year = self._parse_start_year_main_thread()
        budget_label = self.budget_choice.get()
        end_year = datetime.now().year

        self.set_status("Refreshing…")

        t = threading.Thread(
            target=self._refresh_worker,
            args=(start_year, end_year, budget_label),
            daemon=True,
        )
        t.start()

    def _refresh_worker(self, start_year: int, end_year: int, budget_label: str) -> None:
        """
        Background thread: DO NOT TOUCH TKINTER HERE.
        """
        try:
            snap = fetch_latest_noaa_daily_ppm()
            emissions = fetch_world_emissions_owid(start_year, end_year)
            self._q.put(("ok", snap, emissions, start_year, end_year, budget_label))
        except Exception:
            tb = traceback.format_exc()
            self._q.put(("err", tb))

    def _poll_queue(self) -> None:
        """
        Main thread: handle worker results and update UI.
        """
        try:
            msg = self._q.get_nowait()
        except queue.Empty:
            self.after(100, self._poll_queue)
            return

        if msg[0] == "ok":
            _, snap, emissions, start_year, end_year, budget_label = msg
            self.last_snapshot = snap
            self.render(snap, emissions, start_year, end_year, budget_label)
            self.set_status(f"Updated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            _, tb = msg
            self.set_status("Error")
            messagebox.showerror("Refresh failed", tb)

        self._refresh_in_flight = False
        self.refresh_btn.configure(state="normal")
        self.after(100, self._poll_queue)

    def render(
        self,
        snap: Co2Snapshot,
        emissions: list[Tuple[int, float]],
        start_year: int,
        end_year: int,
        budget_label: str,
    ) -> None:
        ppm = snap.ppm
        gt_atm = gtco2_in_atmosphere_from_ppm(ppm)
        above_pre = ppm - PREINDUSTRIAL_PPM

        budget_gt = BUDGETS_GTCO2.get(budget_label, list(BUDGETS_GTCO2.values())[0])

        used_gt = sum(v for _, v in emissions) if emissions else 0.0
        remaining_gt = budget_gt - used_gt

        self._set_card(
            "Latest CO₂ (ppm)",
            f"{ppm:.2f} ppm",
            f"NOAA Mauna Loa daily mean, latest valid date: {snap.date.date()}",
        )

        self._set_card(
            "CO₂ in atmosphere (GtCO₂)",
            f"{gt_atm:,.0f} GtCO₂",
            f"Derived: {ppm:.2f} × {PPM_TO_GTCO2_IN_ATMOSPHERE:.5f} GtCO₂/ppm",
        )

        self._set_card(
            "Above pre-industrial (ppm)",
            f"{above_pre:.2f} ppm",
            f"Using {PREINDUSTRIAL_PPM:.0f} ppm as pre-industrial reference",
        )

        self._set_card(
            "Estimated budget used (GtCO₂)",
            f"{used_gt:,.1f} GtCO₂",
            f"Sum of annual global emissions from {start_year}–{end_year} (OWID)",
        )

        self._set_card(
            "Estimated budget remaining (GtCO₂)",
            f"{remaining_gt:,.1f} GtCO₂",
            "Negative means overshoot (would require net-negative emissions / removals)",
        )

    def _set_card(self, title: str, main: str, sub: str) -> None:
        value, subtitle = self.cards[title]
        value.configure(text=main)
        subtitle.configure(text=sub)


if __name__ == "__main__":
    # Theme polish
    try:
        style = ttk.Style()
        if "clam" in style.theme_names():
            style.theme_use("clam")
    except Exception:
        pass

    App().mainloop()

