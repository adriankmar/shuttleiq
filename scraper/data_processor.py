"""
Data processor: converts raw scraped data into the three required CSVs.

Output files
────────────
  data/processed/matches.csv
  data/processed/tournaments.csv
  data/processed/players.csv

Column specs
────────────
matches.csv:
  match_id, tournament_id, round, player_1, player_2, winner, score,
  p1_world_ranking, p2_world_ranking, p1_seeding, p2_seeding, date

tournaments.csv:
  tournament_id, name, tier, location, date, year

players.csv:
  player_id, name, nationality, current_world_ranking, matches_played, win_rate

Notes on world rankings
───────────────────────
The BWF draw pages do NOT show world rankings — only tournament seedings.
Phase 1 approach:
  - p1_world_ranking / p2_world_ranking are populated from the CURRENT
    rankings snapshot (scraped separately).
  - Seedings are stored in p1_seeding / p2_seeding and are the best proxy
    for relative ranking at time of the tournament.
  - Flag: columns may be NaN for players not found in the current rankings.
  TODO (Phase 2): collect weekly ranking snapshots and join by player + date.
"""

import re
import logging
import pandas as pd
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

from scraper.calendar_scraper import TournamentInfo
from scraper.draw_scraper import DrawData, MatchResult
from scraper.rankings_scraper import RankingEntry

logger = logging.getLogger(__name__)

PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalise_name(name: str) -> str:
    """Normalise player name for matching across data sources."""
    return re.sub(r"\s+", " ", name.strip().upper())


def _score_to_storage(score: str) -> str:
    """
    Convert internal score format to human-readable storage format.
    "21-15 21-18"     → "21-15 21-18"   (unchanged, space-separated)
    "21-15, 21-18"    → "21-15 21-18"   (strip commas)
    "RET"             → "RET"
    """
    if not score:
        return ""
    # Remove commas and normalise whitespace
    return re.sub(r",\s*", " ", score).strip()


def _build_player_id(name: str) -> str:
    """Create a stable player ID slug from a name."""
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower().strip())
    return slug.strip("-")


# ── Core processing functions ─────────────────────────────────────────────────

def build_tournaments_df(tournament_infos: list[TournamentInfo]) -> pd.DataFrame:
    """Build the tournaments.csv DataFrame."""
    rows = []
    for t in tournament_infos:
        rows.append({
            "tournament_id":  t.tournament_id,
            "name":           t.name,
            "tier":           t.tier,
            "location":       t.location,
            "date":           t.date,
            "year":           t.year,
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["tournament_id"])
    df = df.sort_values(["year", "date"]).reset_index(drop=True)
    return df


def build_matches_df(
    draw_results: list[DrawData],
    tournament_map: dict[int, TournamentInfo],
    ranking_map: dict[str, int],   # normalised_name → world_rank
) -> pd.DataFrame:
    """
    Build the matches.csv DataFrame from all draw results.

    ranking_map is used to fill p1_world_ranking / p2_world_ranking where
    available. It may be empty for a partial Phase-1 run.
    """
    rows = []
    match_id_counter = 1

    for draw in draw_results:
        if draw.error or not draw.matches:
            continue

        t_info = tournament_map.get(draw.tournament_id)
        t_date = t_info.date if t_info else ""

        for match in draw.matches:
            p1 = match.player1
            p2 = match.player2
            winner_name = p1.name if match.winner == 1 else p2.name

            p1_rank = ranking_map.get(_normalise_name(p1.name))
            p2_rank = ranking_map.get(_normalise_name(p2.name))

            rows.append({
                "match_id":          match_id_counter,
                "tournament_id":     draw.tournament_id,
                "round":             match.round_name,
                "player_1":          p1.name,
                "player_2":          p2.name,
                "winner":            winner_name,
                "score":             _score_to_storage(match.score),
                "p1_world_ranking":  p1_rank,   # NaN if not in current rankings
                "p2_world_ranking":  p2_rank,
                "p1_seeding":        p1.seeding,
                "p2_seeding":        p2.seeding,
                "date":              t_date,
            })
            match_id_counter += 1

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Enforce dtypes — ranking/seeding cols can be Int64 (nullable int)
    for col in ["p1_world_ranking", "p2_world_ranking", "p1_seeding", "p2_seeding"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df


def build_players_df(
    matches_df: pd.DataFrame,
    ranking_map: dict[str, int],
    nationality_map: dict[str, str],  # normalised_name → nationality
) -> pd.DataFrame:
    """
    Build the players.csv DataFrame by aggregating match results.

    player_id  : stable slug derived from name
    name       : display name (first appearance in matches)
    nationality: 3-letter BWF country code
    current_world_ranking: from ranking_map, NaN if unknown
    matches_played: count of MS matches in dataset
    win_rate   : wins / matches_played
    """
    if matches_df.empty:
        return pd.DataFrame(columns=[
            "player_id", "name", "nationality",
            "current_world_ranking", "matches_played", "win_rate",
        ])

    # Collect all player appearances
    p1_df = matches_df[["player_1", "winner"]].rename(columns={"player_1": "name"})
    p1_df["won"] = (matches_df["player_1"] == matches_df["winner"]).astype(int)

    p2_df = matches_df[["player_2", "winner"]].rename(columns={"player_2": "name"})
    p2_df["won"] = (matches_df["player_2"] == matches_df["winner"]).astype(int)

    all_appearances = pd.concat([p1_df[["name", "won"]], p2_df[["name", "won"]]], ignore_index=True)
    stats = (
        all_appearances
        .groupby("name", as_index=False)
        .agg(matches_played=("name", "count"), wins=("won", "sum"))
    )
    stats["win_rate"] = (stats["wins"] / stats["matches_played"]).round(4)
    stats["player_id"] = stats["name"].apply(_build_player_id)

    norm_name = stats["name"].apply(_normalise_name)
    stats["nationality"] = norm_name.map(nationality_map).fillna("")
    stats["current_world_ranking"] = norm_name.map(ranking_map)
    stats["current_world_ranking"] = pd.to_numeric(
        stats["current_world_ranking"], errors="coerce"
    ).astype("Int64")

    return stats[[
        "player_id", "name", "nationality",
        "current_world_ranking", "matches_played", "win_rate",
    ]].sort_values("current_world_ranking", na_position="last").reset_index(drop=True)


# ── Main entry point ──────────────────────────────────────────────────────────

def process_and_save(
    tournament_infos: list[TournamentInfo],
    draw_results: list[DrawData],
    rankings: list[RankingEntry],
    output_dir: Path = PROCESSED_DIR,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Build all three DataFrames, save to CSV, and return them.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Build lookup maps
    tournament_map = {t.tournament_id: t for t in tournament_infos}

    ranking_map: dict[str, int] = {
        _normalise_name(r.name): r.rank for r in rankings
    }

    # Build nationality map from draw data
    nationality_map: dict[str, str] = {}
    for draw in draw_results:
        for match in draw.matches:
            for player in [match.player1, match.player2]:
                if player.nationality:
                    nationality_map[_normalise_name(player.name)] = player.nationality

    # Also populate from rankings data
    for r in rankings:
        if r.nationality:
            nationality_map[_normalise_name(r.name)] = r.nationality

    # Build DataFrames
    tournaments_df = build_tournaments_df(tournament_infos)
    matches_df     = build_matches_df(draw_results, tournament_map, ranking_map)
    players_df     = build_players_df(matches_df, ranking_map, nationality_map)

    # Save
    t_path = output_dir / "tournaments.csv"
    m_path = output_dir / "matches.csv"
    p_path = output_dir / "players.csv"

    tournaments_df.to_csv(t_path, index=False)
    matches_df.to_csv(m_path, index=False)
    players_df.to_csv(p_path, index=False)

    logger.info("Saved %d tournaments  → %s", len(tournaments_df), t_path)
    logger.info("Saved %d matches      → %s", len(matches_df), m_path)
    logger.info("Saved %d players      → %s", len(players_df), p_path)

    return tournaments_df, matches_df, players_df
