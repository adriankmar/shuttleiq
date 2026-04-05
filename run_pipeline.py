"""
ShuttleIQ Data Pipeline — Phase 1
===================================
Collects BWF World Tour Men's Singles match data (2022–present) and outputs
three clean CSVs: matches.csv, tournaments.csv, players.csv.

Usage
─────
    # Full run (all years, ~60 tournaments, takes 20-40 min)
    python run_pipeline.py

    # Quick test: single tournament
    python run_pipeline.py --test

    # Specific years only
    python run_pipeline.py --years 2024 2025

    # Skip re-scraping if raw data already exists
    python run_pipeline.py --years 2024 --skip-if-cached

Setup
─────
    pip install -r requirements.txt
    playwright install chromium

Data Sources
────────────
  Primary: https://bwfworldtour.bwfbadminton.com
    - Calendar pages for tournament IDs/metadata
    - Full draw pages (/draws/full-draw/ms) for match brackets and scores
    - Rankings page for current world rankings

  The site is WordPress + Vue.js and returns 403 to plain HTTP requests,
  so we use Playwright (headless Chromium) to render each page.

Known Limitations (flagged for Phase 2)
────────────────────────────────────────
  1. WORLD RANKINGS: The draw pages only show seedings, not world rankings.
     p1_world_ranking / p2_world_ranking are populated from a CURRENT
     rankings snapshot. For historical accuracy, collect weekly snapshots
     and join by player + tournament date.

  2. SCORE PERSPECTIVE: Scores are shown from the WINNER's perspective on
     the draw sheet. Verify a few results against official records if using
     game-level margin calculations for PAR.

  3. R64/QUALIFYING: Some Super 1000 events have a R64 first round. The
     scraper handles this but those matches may have lower data quality.

  4. RETIREMENTS/WALKOVERS: Stored as score="RET" or "W.O." — the PAR
     formula should handle these as partial match data.
"""

import asyncio
import json
import logging
import argparse
import sys
from pathlib import Path

import pandas as pd

# Add project root to path so imports work when running from project dir
sys.path.insert(0, str(Path(__file__).parent))

from scraper.calendar_scraper import scrape_calendar, TournamentInfo
from scraper.draw_scraper import scrape_all_draws, DrawData
from scraper.rankings_scraper import get_current_rankings
from scraper.data_processor import process_and_save

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
RAW_DIR       = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

CACHE_TOURNAMENTS = RAW_DIR / "tournaments_raw.json"
CACHE_DRAWS       = RAW_DIR / "draws_raw.json"
CACHE_RANKINGS    = RAW_DIR / "rankings_raw.json"


# ── Cache helpers ─────────────────────────────────────────────────────────────

def _save_json(path: Path, obj) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, default=str)


def _load_json(path: Path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _tournaments_to_json(items: list[TournamentInfo]) -> list[dict]:
    return [
        {
            "tournament_id": t.tournament_id,
            "name":          t.name,
            "slug":          t.slug,
            "tier":          t.tier,
            "location":      t.location,
            "date":          t.date,
            "year":          t.year,
        }
        for t in items
    ]


def _tournaments_from_json(data: list[dict]) -> list[TournamentInfo]:
    return [TournamentInfo(**d) for d in data]


def _draws_to_json(draws: list[DrawData]) -> list[dict]:
    result = []
    for d in draws:
        matches = []
        for m in d.matches:
            matches.append({
                "match_num":    m.match_num,
                "round_name":   m.round_name,
                "p1_name":      m.player1.name,
                "p1_nat":       m.player1.nationality,
                "p1_seeding":   m.player1.seeding,
                "p1_bwf_id":    m.player1.bwf_id,
                "p1_bwf_url":   m.player1.bwf_url,
                "p2_name":      m.player2.name,
                "p2_nat":       m.player2.nationality,
                "p2_seeding":   m.player2.seeding,
                "p2_bwf_id":    m.player2.bwf_id,
                "p2_bwf_url":   m.player2.bwf_url,
                "winner":       m.winner,
                "score":        m.score,
                "duration_min": m.duration_min,
            })
        result.append({
            "tournament_id": d.tournament_id,
            "slug":          d.slug,
            "error":         d.error,
            "matches":       matches,
        })
    return result


def _draws_from_json(data: list[dict]) -> list[DrawData]:
    from scraper.draw_scraper import DrawData, MatchResult, PlayerEntry
    draws = []
    for d in data:
        matches = []
        for m in d.get("matches", []):
            matches.append(MatchResult(
                match_num=m["match_num"],
                round_name=m["round_name"],
                player1=PlayerEntry(
                    name=m["p1_name"], nationality=m["p1_nat"],
                    seeding=m["p1_seeding"], bwf_id=m["p1_bwf_id"],
                    bwf_url=m["p1_bwf_url"],
                ),
                player2=PlayerEntry(
                    name=m["p2_name"], nationality=m["p2_nat"],
                    seeding=m["p2_seeding"], bwf_id=m["p2_bwf_id"],
                    bwf_url=m["p2_bwf_url"],
                ),
                winner=m["winner"],
                score=m["score"],
                duration_min=m["duration_min"],
            ))
        draws.append(DrawData(
            tournament_id=d["tournament_id"],
            slug=d["slug"],
            matches=matches,
            error=d.get("error"),
        ))
    return draws


# ── Pipeline steps ────────────────────────────────────────────────────────────

async def step_calendar(years: list[int], skip_if_cached: bool) -> list[TournamentInfo]:
    """Step 1: get tournament list from BWF calendar."""
    if skip_if_cached and CACHE_TOURNAMENTS.exists():
        logger.info("Loading cached tournament list from %s", CACHE_TOURNAMENTS)
        return _tournaments_from_json(_load_json(CACHE_TOURNAMENTS))

    logger.info("=== Step 1: Scraping calendar for years %s ===", years)
    tournaments = await scrape_calendar(years)
    _save_json(CACHE_TOURNAMENTS, _tournaments_to_json(tournaments))
    logger.info("Cached %d tournaments → %s", len(tournaments), CACHE_TOURNAMENTS)
    return tournaments


async def step_draws(
    tournaments: list[TournamentInfo],
    skip_if_cached: bool,
    rate_limit: float,
) -> list[DrawData]:
    """Step 2: scrape match draws for each tournament."""
    if skip_if_cached and CACHE_DRAWS.exists():
        logger.info("Loading cached draw data from %s", CACHE_DRAWS)
        return _draws_from_json(_load_json(CACHE_DRAWS))

    logger.info("=== Step 2: Scraping MS draws for %d tournaments ===", len(tournaments))
    draws = await scrape_all_draws(tournaments, rate_limit_sec=rate_limit)

    ok  = sum(1 for d in draws if not d.error and d.matches)
    err = sum(1 for d in draws if d.error)
    logger.info("Draws: %d OK, %d errors", ok, err)

    _save_json(CACHE_DRAWS, _draws_to_json(draws))
    logger.info("Cached draw data → %s", CACHE_DRAWS)
    return draws


async def step_rankings(skip_if_cached: bool):
    """Step 3: get current world rankings."""
    if skip_if_cached and CACHE_RANKINGS.exists():
        logger.info("Loading cached rankings from %s", CACHE_RANKINGS)
        from scraper.rankings_scraper import RankingEntry
        data = _load_json(CACHE_RANKINGS)
        return [RankingEntry(**r) for r in data]

    logger.info("=== Step 3: Scraping current world rankings ===")
    rankings = await get_current_rankings()

    _save_json(CACHE_RANKINGS, [
        {"rank": r.rank, "name": r.name, "nationality": r.nationality,
         "points": r.points, "bwf_id": r.bwf_id, "bwf_url": r.bwf_url}
        for r in rankings
    ])
    logger.info("Cached %d ranking entries → %s", len(rankings), CACHE_RANKINGS)
    return rankings


def step_process(
    tournaments: list[TournamentInfo],
    draws: list[DrawData],
    rankings,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Step 4: process raw data → CSVs."""
    logger.info("=== Step 4: Building CSVs ===")
    return process_and_save(tournaments, draws, rankings, PROCESSED_DIR)


def print_samples(
    tournaments_df: pd.DataFrame,
    matches_df: pd.DataFrame,
    players_df: pd.DataFrame,
) -> None:
    """Print a sample from each CSV to verify the output."""
    sep = "─" * 80
    print(f"\n{sep}")
    print("TOURNAMENTS.CSV  (first 5 rows)")
    print(sep)
    print(tournaments_df.head().to_string(index=False))

    print(f"\n{sep}")
    print("MATCHES.CSV  (first 10 rows)")
    print(sep)
    print(matches_df.head(10).to_string(index=False))

    print(f"\n{sep}")
    print("PLAYERS.CSV  (top 15 by ranking)")
    print(sep)
    print(players_df.head(15).to_string(index=False))

    print(f"\n{sep}")
    print(f"TOTALS:  {len(tournaments_df)} tournaments  |  "
          f"{len(matches_df)} matches  |  {len(players_df)} players")
    print(sep)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main(args: argparse.Namespace) -> None:
    years = args.years if args.years else list(range(2022, 2027))
    skip  = args.skip_if_cached
    rate  = 3.0  # seconds between requests

    if args.test:
        logger.info("TEST MODE: scraping only Malaysia Open 2026 (id=5227)")
        from scraper.draw_scraper import DrawData
        from scraper.calendar_scraper import TournamentInfo

        # Hard-code one completed tournament for quick smoke test
        test_tournament = TournamentInfo(
            tournament_id=5227,
            name="PETRONAS Malaysia Open 2026",
            slug="petronas-malaysia-open-2026",
            tier="Super1000",
            location="Kuala Lumpur, Malaysia",
            date="2026-01-06",
            year=2026,
        )
        tournaments = [test_tournament]
        draws       = await step_draws(tournaments, skip_if_cached=skip, rate_limit=rate)
        rankings    = await step_rankings(skip_if_cached=skip)
    else:
        tournaments = await step_calendar(years, skip_if_cached=skip)
        draws       = await step_draws(tournaments, skip_if_cached=skip, rate_limit=rate)
        rankings    = await step_rankings(skip_if_cached=skip)

    t_df, m_df, p_df = step_process(tournaments, draws, rankings)
    print_samples(t_df, m_df, p_df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ShuttleIQ Phase 1 Data Pipeline")
    parser.add_argument("--test",           action="store_true",
                        help="Quick test: scrape only one tournament")
    parser.add_argument("--years",          type=int, nargs="+",
                        help="Years to scrape (default: 2022–2026)")
    parser.add_argument("--skip-if-cached", action="store_true",
                        help="Reuse cached raw data if it exists")
    args = parser.parse_args()

    asyncio.run(main(args))
