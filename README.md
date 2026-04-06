# 🏸 ShuttleIQ

**BWF badminton analytics platform** — tracks performance across Men's and Women's Singles on the BWF World Tour using a custom PAR (Performance Above Replacement) rating system.

## 🌐 Live Demo

[**shuttleiq.streamlit.app**](https://shuttleiq.streamlit.app)

---

## 📊 What is PAR?

**Performance Above Replacement** measures how much better (or worse) a player performs compared to a hypothetical "replacement-level" player — i.e., someone who just barely qualifies for BWF World Tour draws.

PAR is calculated per match, then aggregated into a career score. Higher PAR = consistently beating stronger opponents, in dominant fashion, at prestigious tournaments.

### PAR Components

| Component | What it measures |
|---|---|
| **Base Result** | Win = 1.0, Loss = 0.0 |
| **Opponent Multiplier** | Scaled by opponent's world ranking — beating a top-10 player scores higher than beating a qualifier |
| **Dominance Score** | Game-score margin bonus — a 21-10 21-12 win scores higher than 21-19 21-19 |
| **Tournament Tier** | Super 1000 > Super 750 > Super 500 > Super 300 > BWF World Tour |

A player's final PAR score is the average match PAR across their career, minus the replacement level baseline.

### PAR Tiers

| Tier | PAR Score |
|---|---|
| 🥇 Elite | ≥ 0.54 |
| 📈 Above Average | ≥ 0.30 |
| ➡️ Average | ≥ 0.09 |
| 📉 Below Average | < 0.09 |

Thresholds are calibrated empirically against the actual score distribution of qualified players (15+ matches).

---

## 📈 Key Stats

- **400+** players tracked (MS + WS combined)
- **5,254** matches across 85+ tournaments
- **2023–2026** BWF World Tour coverage
- Updated monthly via automated scraper

---

## 🔧 Tech Stack

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Pandas](https://img.shields.io/badge/Pandas-2.0-150458?logo=pandas)
![Playwright](https://img.shields.io/badge/Playwright-headless-2EAD33?logo=playwright)
![Streamlit](https://img.shields.io/badge/Streamlit-1.35-FF4B4B?logo=streamlit)
![Plotly](https://img.shields.io/badge/Plotly-5.22-3F4F75?logo=plotly)

| Layer | Tool |
|---|---|
| Data collection | Playwright (headless Chromium) |
| Data processing | Pandas |
| PAR model | Custom Python (`model/par_calculator.py`) |
| Dashboard | Streamlit + Plotly |
| Deployment | Streamlit Community Cloud |

---

## 🚀 How to Run Locally

```bash
# 1. Clone the repo
git clone https://github.com/adriankmar/shuttleiq.git
cd shuttleiq

# 2. Install dependencies
pip install -r requirements.txt
playwright install chromium

# 3. Launch the dashboard (uses pre-built data/processed/ CSVs)
streamlit run dashboard/app.py
```

> The `data/processed/` and `data/par_*.csv` files are committed to the repo — you can run the dashboard immediately without re-scraping.

---

## 🔄 Data Refresh

To re-scrape BWF, recalculate PAR scores, and push updated data:

```bash
./refresh.sh
```

This script:
1. Scrapes BWF World Tour calendar + draws for 2023–2026 (MS + WS)
2. Recalculates PAR scores for both disciplines
3. Commits and pushes updated CSVs to GitHub

The live site updates automatically within ~2 minutes of the push.

---

## 📁 Project Structure

```
shuttleiq/
├── scraper/
│   ├── calendar_scraper.py   # BWF calendar → tournament list
│   ├── draw_scraper.py       # Tournament draws → match results
│   ├── rankings_scraper.py   # Current BWF world rankings
│   └── data_processor.py    # Raw data → clean CSVs
├── model/
│   └── par_calculator.py    # PAR score computation
├── dashboard/
│   └── app.py               # Streamlit dashboard
├── data/
│   ├── processed/           # Match, player, tournament CSVs
│   └── par_scores_*.csv     # Final PAR scores per discipline
├── run_pipeline.py          # Full scrape + process pipeline
├── refresh.sh               # One-command data refresh
└── requirements.txt
```
