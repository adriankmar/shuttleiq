"""
PAR (Performance Above Replacement) Calculator — ShuttleIQ Phase 2
====================================================================

Reads the three Phase 1 CSVs and produces two output files:

  data/par_scores.csv    — one row per player
  data/par_timeline.csv  — one row per player per tournament

Formula overview
----------------
Each match contributes a Match Score to every player who appeared in it:

    Match Score = (Base Result × Opponent Multiplier × Tier Multiplier)
                  + Dominance Score

  Base Result         : Win = 1.0, Loss = 0.0
  Opponent Multiplier : seeding-based scale (0.5 – 2.0); world ranking fallback
  Tier Multiplier     : Super1000=1.20, Super750=1.10, Super500=1.05,
                        Super300=1.00, Finals=1.25
  Dominance Score     : 0.0–1.0 from point margins; inverted for losses

PAR
---
  Replacement level   = mean Match Score of all "replacement-level" match
                        appearances (players who are unseeded AND unranked
                        in the current snapshot, or seeded 9+)
  PAR                 = Player's mean Match Score − Replacement Level

Usage
-----
  python model/par_calculator.py
  python model/par_calculator.py --data-dir /custom/path/to/data
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# ── Path defaults ─────────────────────────────────────────────────────────────

_THIS_FILE  = Path(__file__).resolve()
_MODEL_DIR  = _THIS_FILE.parent                         # .../shuttleiq/model/
_PROJECT    = _MODEL_DIR.parent                         # .../shuttleiq/
_DATA_DIR   = _PROJECT / "data"
_PROC_DIR   = _DATA_DIR / "processed"

TOURNAMENTS_CSV = _PROC_DIR / "tournaments.csv"


# ── Constants ─────────────────────────────────────────────────────────────────

TIER_MULTIPLIER: dict[str, float] = {
    "Super1000": 1.20,
    "Super750":  1.10,
    "Super500":  1.05,
    "Super300":  1.00,
    "Finals":    1.25,
}

# Seeding brackets → opponent strength multiplier when that player is the OPPONENT
# "unseeded_ranked_top15" handled separately using world_ranking ≤ 15
SEEDING_MULTIPLIER: list[tuple[range | None, float]] = [
    (range(1, 3),   2.0),   # seeds 1-2
    (range(3, 5),   1.7),   # seeds 3-4
    (range(5, 9),   1.4),   # seeds 5-8
]
MULTIPLIER_UNSEEDED_TOP15  = 1.2   # unseeded but ranked ≤ 15
MULTIPLIER_UNSEEDED        = 1.0   # unseeded, ranked 16+, or unranked
MULTIPLIER_LOSS_TO_UNSEEDED = 0.5  # penalty for losing to unseeded/unranked

# Margin → dominance score mapping (applied to winner's margin; inverted for loser)
# Each entry: (max_margin_inclusive, dominance_value)
MARGIN_BANDS: list[tuple[int, float]] = [
    (3,  0.2),
    (6,  0.4),
    (10, 0.6),
    (15, 0.8),
    (999, 1.0),   # 16+
]

# A player is "replacement level" if they are unseeded (no tournament seed) AND
# either unranked in the current snapshot or ranked 9+.
# This threshold controls who counts as "replacement".
REPLACEMENT_SEED_THRESHOLD = 9   # seeds 9+ are treated as replacement-level anchors


# ── Helper functions ──────────────────────────────────────────────────────────

def _opponent_multiplier(
    opp_seeding: Optional[float],
    opp_ranking: Optional[float],
    player_won: bool,
) -> float:
    """
    Determine the opponent strength multiplier for a single match appearance.

    opp_seeding  : tournament seed of the opponent (NaN / None if unseeded)
    opp_ranking  : current world ranking of the opponent (NaN / None if unknown)
    player_won   : whether the focal player won this match

    Returns a float in [0.5, 2.0].
    """
    seed = None if (opp_seeding is None or pd.isna(opp_seeding)) else int(opp_seeding)
    rank = None if (opp_ranking is None or pd.isna(opp_ranking)) else int(opp_ranking)

    # Seeding takes priority
    if seed is not None:
        for seed_range, mult in SEEDING_MULTIPLIER:
            if seed in seed_range:
                return mult
        # Seed 9+ but still seeded → treat as between unseeded_top15 and unseeded
        # Use 1.1 for seeded-but-outside-top-8 (below 1.2 unseeded-top15 since seedings
        # at a Super1000 typically start at ~rank 12-15 for seed 9)
        return 1.1

    # No seeding — fall back to world ranking
    if rank is not None and rank <= 15:
        return MULTIPLIER_UNSEEDED_TOP15

    # Unseeded AND unranked (or ranked 16+)
    if not player_won:
        return MULTIPLIER_LOSS_TO_UNSEEDED   # 0.5 — upset loss penalty
    return MULTIPLIER_UNSEEDED               # 1.0


RET_DOMINANCE_CAP = 0.3   # dominance ceiling for retirement / walkover matches


def _dominance_score(score_str: str, player_won: bool) -> Optional[float]:
    """
    Parse a score string and return a dominance value in [0.0, 1.0].

    Retirement / walkover handling:
        The match was incomplete, so we cannot fairly measure dominance from
        the partial scoreline.  We return RET_DOMINANCE_CAP (0.3) for both
        winner and loser — the winner still earns Base Result = 1.0, the loser
        earns 0.0, but neither gets inflated/deflated dominance credit.

    For a normal WIN  : higher margin → higher dominance (closer to 1.0)
    For a normal LOSS : higher opponent margin → lower dominance (closer to 0.0),
                        representing "how competitive was the loss"
                        (a close 3-game loss ≈ 0.8; a bagel loss ≈ 0.1)

    Returns None only when the score string is entirely absent/unparseable
    (those appearances are omitted from PAR).
    """
    if not isinstance(score_str, str) or not score_str.strip():
        return None

    upper = score_str.strip().upper()
    if upper in ("RET", "W.O.", "W/O", "RETIRED", "WALKOVER"):
        # Incomplete match — cap dominance for both players
        return RET_DOMINANCE_CAP

    # Parse partial scores that contain game integers followed by RET/WO text,
    # e.g. "21-23 1-6" from a mid-match retirement.  Treat as partial score.
    has_ret = "RET" in upper or "W.O" in upper or "W/O" in upper

    games = score_str.strip().split()
    margins: list[int] = []
    for game in games:
        parts = game.split("-")
        if len(parts) != 2:
            continue
        try:
            a, b = int(parts[0]), int(parts[1])
        except ValueError:
            continue
        margins.append(abs(a - b))

    if not margins:
        return None

    avg_margin = sum(margins) / len(margins)

    # Map average margin to dominance band
    raw_dom = 0.2  # default floor
    for max_margin, dom_val in MARGIN_BANDS:
        if avg_margin <= max_margin:
            raw_dom = dom_val
            break

    if player_won:
        dom = raw_dom
    else:
        # Invert: tight loss → high competitive score; blowout loss → low
        dom = round(1.0 - raw_dom, 2)

    # If the score string contained a partial retirement, cap dominance
    if has_ret:
        dom = min(dom, RET_DOMINANCE_CAP)

    return dom


def _match_score(
    base_result: float,
    opp_mult: float,
    tier_mult: float,
    dominance: Optional[float],
) -> float:
    """
    Combine the four components into a single match score.

    Dominance adds on top of the multiplicative term so that even a loss
    to a top-2 seed in a tight 3-gamer still earns meaningful credit.
    """
    dom = dominance if dominance is not None else 0.0
    return round((base_result * opp_mult * tier_mult) + dom, 4)


# ── Core computation ──────────────────────────────────────────────────────────

def build_match_appearances(
    matches: pd.DataFrame,
    tournaments: pd.DataFrame,
) -> pd.DataFrame:
    """
    Expand each match row into TWO appearance rows (one per player),
    computing match_score, dominance, opp_mult, and tier_mult for each.

    Returns a DataFrame with columns:
        player_name, tournament_id, tournament_name, tier, date,
        round, won, opp_seeding, opp_ranking, opp_mult, tier_mult,
        dominance, match_score, score, is_ret
    """
    tier_map = tournaments.set_index("tournament_id")["tier"].to_dict()
    tname_map = tournaments.set_index("tournament_id")["name"].to_dict()
    date_map  = tournaments.set_index("tournament_id")["date"].to_dict()

    rows: list[dict] = []

    for _, m in matches.iterrows():
        p1, p2   = m["player_1"], m["player_2"]
        winner   = m["winner"]
        score    = m.get("score", "")
        tid      = m["tournament_id"]
        rnd      = m["round"]

        tier     = tier_map.get(tid, "Super300")
        t_mult   = TIER_MULTIPLIER.get(tier, 1.0)
        t_name   = tname_map.get(tid, str(tid))
        t_date   = date_map.get(tid, "")

        p1_seed  = m.get("p1_seeding")
        p2_seed  = m.get("p2_seeding")
        p1_rank  = m.get("p1_world_ranking")
        p2_rank  = m.get("p2_world_ranking")

        upper_score = str(score).strip().upper() if isinstance(score, str) else ""
        # A "pure" RET/WO score has no game integers at all.
        # Partial scores like "21-23 1-6" (mid-match retirement) are NOT pure RET —
        # _dominance_score handles them by capping at RET_DOMINANCE_CAP.
        is_ret = upper_score in ("RET", "W.O.", "W/O", "RETIRED", "WALKOVER")

        for focal, opp, focal_seed, opp_seed, focal_rank, opp_rank in [
            (p1, p2, p1_seed, p2_seed, p1_rank, p2_rank),
            (p2, p1, p2_seed, p1_seed, p2_rank, p1_rank),
        ]:
            won = (focal == winner)
            base = 1.0 if won else 0.0

            # Score is from P1's perspective; flip for P2
            dom_score_str = score
            if focal == p2 and isinstance(score, str) and score.strip():
                # Reverse each game "a-b" → "b-a" so the formula sees
                # the focal player's points first
                flipped_games = []
                for g in score.strip().split():
                    parts = g.split("-")
                    if len(parts) == 2:
                        flipped_games.append(f"{parts[1]}-{parts[0]}")
                    else:
                        flipped_games.append(g)
                dom_score_str = " ".join(flipped_games)

            dom = _dominance_score(dom_score_str, won)
            opp_mult = _opponent_multiplier(opp_seed, opp_rank, won)
            ms = _match_score(base, opp_mult, t_mult, dom)

            rows.append({
                "player_name":       focal,
                "tournament_id":     tid,
                "tournament_name":   t_name,
                "tier":              tier,
                "date":              t_date,
                "round":             rnd,
                "won":               won,
                "opp_name":          opp,
                "opp_seeding":       opp_seed if not pd.isna(opp_seed) else None,
                "opp_ranking":       opp_rank if not pd.isna(opp_rank) else None,
                "opp_mult":          opp_mult,
                "tier_mult":         t_mult,
                "dominance":         dom,
                "match_score":       ms,
                "score":             score,
                "is_ret":            is_ret,
            })

    df = pd.DataFrame(rows)
    return df


_skipped_global: list[str] = []  # reserved for future diagnostic messages


def compute_par(appearances: pd.DataFrame) -> tuple[pd.DataFrame, float]:
    """
    Given the appearances DataFrame, compute PAR scores per player.

    Returns (par_df, replacement_level).

    Replacement level is defined as the mean match_score of all appearances
    where the focal player is "replacement level":
        — no tournament seeding (opp_seeding is our proxy via opp_mult)
        Actually we need the focal player's own seeding.

    Since we didn't store focal_seeding in appearances, we derive it:
    a player appearance is replacement-level if their opp_mult (when they
    ARE the opponent) would be MULTIPLIER_UNSEEDED (1.0) or below,
    i.e. the player is unseeded and ranked outside top-15.

    Practically we flag an appearance as "replacement level anchor" when:
        is_ret = False  (full match)
        AND the focal player has no seeding in this tournament
        AND (no world ranking OR world ranking > 15)

    We infer focal seeding from: if THEY were P1, their seeding = p1_seeding;
    if P2, p2_seeding.  Since appearances already flipped these, we can
    reconstruct:  if opp_mult for focal (when focal is the opponent) = 1.0,
    they're replacement level.

    Simpler approach: a player is replacement-level in a match when:
        opp_mult_they_would_present = _opponent_multiplier(focal_seed, focal_rank, True)
    But we don't store focal_seed directly.

    ACTUAL approach used: join back to matches to get focal_seeding.
    This function accepts appearances with an optional "focal_seeding" column;
    if missing, replacement level = players whose mean opp_mult_received across
    all matches ≤ 1.1 (i.e., they're never seeded above 8).
    """
    # ── Compute per-player stats ───────────────────────────────────────────
    full_matches = appearances[~appearances["is_ret"] | appearances["dominance"].notna()]

    agg = (
        appearances
        .groupby("player_name")
        .agg(
            matches_played=("match_score", "count"),
            avg_match_score=("match_score", "mean"),
            best_match_score=("match_score", "max"),
        )
        .reset_index()
    )

    # Best win = opponent name in the highest-scoring WIN
    wins = appearances[appearances["won"]].copy()
    if not wins.empty:
        best_win_idx = wins.groupby("player_name")["match_score"].idxmax()
        best_wins = wins.loc[best_win_idx, ["player_name", "opp_name"]].rename(
            columns={"opp_name": "best_win"}
        )
        agg = agg.merge(best_wins, on="player_name", how="left")
    else:
        agg["best_win"] = pd.NA

    # ── Replacement level ─────────────────────────────────────────────────
    # Replacement-level appearances: player had no seeding (their own opp_mult
    # as seen by OTHERS = 1.0 or 0.5). We identify this by looking at matches
    # where this player was the OPPONENT — their opp_mult as seen by the other
    # player's appearance row tells us their strength bracket.
    #
    # In the appearances table, opp_mult is the multiplier applied to the
    # FOCAL player for facing THIS opponent.  So for each player P, the
    # opp_mult values from OTHER players' appearances where opp_name==P
    # tell us how strong P appears to others.
    #
    # P is "replacement level" if every tournament appearance they have shows
    # opp_mult ≤ 1.0 when they are the opponent (i.e., unseeded + unranked/16+).

    # Get the max opp_mult that each player presents as an opponent
    opp_strength = (
        appearances
        .groupby("opp_name")["opp_mult"]
        .max()
        .reset_index()
        .rename(columns={"opp_name": "player_name", "opp_mult": "max_opp_mult_presented"})
    )
    agg = agg.merge(opp_strength, on="player_name", how="left")
    agg["max_opp_mult_presented"] = agg["max_opp_mult_presented"].fillna(1.0)

    # Replacement appearances: from players who never presented > 1.1 strength
    replacement_mask = agg["max_opp_mult_presented"] <= 1.1
    replacement_players = set(agg.loc[replacement_mask, "player_name"])

    replacement_appearances = appearances[
        appearances["player_name"].isin(replacement_players) & ~appearances["is_ret"]
    ]

    if replacement_appearances.empty:
        # Fallback: use bottom half of all players by avg_match_score
        median_score = agg["avg_match_score"].median()
        replacement_level = float(agg.loc[
            agg["avg_match_score"] <= median_score, "avg_match_score"
        ].mean())
    else:
        replacement_level = float(replacement_appearances["match_score"].mean())

    agg["par_score"] = (agg["avg_match_score"] - replacement_level).round(4)

    # ── PAR tier classification ───────────────────────────────────────────
    # Thresholds calibrated to the actual distribution of qualified players
    # (15+ matches, n=71) from the 2023–2026 BWF World Tour dataset:
    #   Elite         ≈ top  5%  (rank  1– 4/71) : PAR ≥ 0.54
    #   Above Average ≈ top  6–30% (rank  5–21/71) : PAR ≥ 0.30
    #   Average       ≈ top 31–70% (rank 22–50/71) : PAR ≥ 0.09
    #   Below Average ≈ bottom 30% (rank 51–71/71) : PAR <  0.09
    def _par_tier(par: float) -> str:
        if par >= 0.54:  return "Elite"
        if par >= 0.30:  return "Above Average"
        if par >= 0.09:  return "Average"
        return "Below Average"

    agg["par_tier"] = agg["par_score"].apply(_par_tier)

    # ── Nationality ───────────────────────────────────────────────────────
    # Pull nationality from players.csv via the appearances (not stored there,
    # so we return agg without it; caller joins)

    return agg, replacement_level


def compute_timeline(appearances: pd.DataFrame, replacement_level: float) -> pd.DataFrame:
    """
    PAR score per player per tournament.

    tournament_par = (mean match_score in this tournament) - replacement_level
    """
    tl = (
        appearances
        .groupby(["player_name", "tournament_name", "tier", "date"])
        .agg(
            matches_in_tournament=("match_score", "count"),
            avg_match_score_in_tourney=("match_score", "mean"),
        )
        .reset_index()
    )
    tl["tournament_par"] = (tl["avg_match_score_in_tourney"] - replacement_level).round(4)
    tl = tl.drop(columns=["avg_match_score_in_tourney"])
    tl = tl.sort_values(["player_name", "date"]).reset_index(drop=True)
    return tl


# ── I/O helpers ───────────────────────────────────────────────────────────────

def _load_csvs(data_dir: Path, discipline: str = "ms") -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    proc = data_dir / "processed"
    disc = discipline.lower()
    matches     = pd.read_csv(proc / f"matches_{disc}.csv")
    tournaments = pd.read_csv(proc / "tournaments.csv")
    players     = pd.read_csv(proc / f"players_{disc}.csv")

    # Coerce numeric columns that may have loaded as object/float with NaN
    for col in ["p1_world_ranking", "p2_world_ranking", "p1_seeding", "p2_seeding"]:
        matches[col] = pd.to_numeric(matches[col], errors="coerce")

    return matches, tournaments, players


def _print_results(
    par_df: pd.DataFrame,
    replacement_level: float,
    skipped: list[str],
    discipline: str = "MS",
) -> None:
    """Pretty-print PAR summary to stdout."""
    SEP = "─" * 72

    print(f"\n{SEP}")
    print(f"  ShuttleIQ PAR Calculator — {discipline.upper()} Results")
    print(SEP)
    print(f"\n  Replacement level baseline : {replacement_level:.4f}")
    print(f"  (mean match score of unseeded / unranked appearances)\n")

    # Distribution stats for qualified players (15+ matches)
    qualified = par_df[par_df["matches_played"] >= 15]["par_score"]
    print(f"  PAR Distribution — qualified players (≥15 matches, n={len(qualified)})")
    print(f"    Min   : {qualified.min():+.4f}")
    print(f"    Max   : {qualified.max():+.4f}")
    print(f"    Mean  : {qualified.mean():+.4f}")
    print(f"    p75   : {qualified.quantile(0.75):+.4f}")
    print(f"    p90   : {qualified.quantile(0.90):+.4f}")
    print(f"    p95   : {qualified.quantile(0.95):+.4f}")
    print(f"  Tier thresholds: Elite≥0.54 | Above Average≥0.30 | Average≥0.09 | Below Average<0.09")
    tier_counts = par_df[par_df["matches_played"] >= 15]["par_tier"].value_counts()
    for t in ["Elite", "Above Average", "Average", "Below Average"]:
        n = tier_counts.get(t, 0)
        print(f"    {t:<14}: {n:3d} players ({n/len(qualified)*100:.0f}%)")
    print()

    # Sort by PAR descending
    ranked = par_df.sort_values("par_score", ascending=False).reset_index(drop=True)
    ranked.index += 1  # 1-based rank

    cols = ["player_name", "nationality", "matches_played",
            "avg_match_score", "par_score", "par_tier"]

    def _fmt_row(rank: int, row: pd.Series) -> str:
        name  = str(row.get("player_name", ""))[:22]
        nat   = str(row.get("nationality", ""))[:3]
        mp    = int(row.get("matches_played", 0))
        avg   = f"{row.get('avg_match_score', 0):.3f}"
        par   = f"{row.get('par_score', 0):+.3f}"
        tier  = str(row.get("par_tier", ""))
        bw    = str(row.get("best_win", ""))[:20]
        return (f"  {rank:>3}.  {name:<22} {nat:<4} "
                f"MP:{mp:<3}  Avg:{avg}  PAR:{par:<8}  [{tier}]  Best win: {bw}")

    print(f"  TOP 10 PAR RANKINGS")
    print(f"  {'#':>3}   {'Player':<22} {'Nat':<4} {'':17} {'PAR':<10}  {'Tier'}")
    print(f"  {'-'*68}")
    for i, (_, row) in enumerate(ranked.head(10).iterrows(), 1):
        print(_fmt_row(i, row))

    print(f"\n  BOTTOM 5 PAR RANKINGS")
    print(f"  {'-'*68}")
    bottom = ranked.tail(5).iloc[::-1].reset_index(drop=True)
    for i, (_, row) in enumerate(bottom.iterrows(), 1):
        rank_pos = len(ranked) - 4 + i
        print(_fmt_row(rank_pos, row))

    if skipped:
        print(f"\n  INCOMPLETE DATA — SKIPPED DOMINANCE ({len(skipped)} match(es))")
        for s in skipped:
            print(s)

    print(f"\n{SEP}\n")


# ── Entry point ───────────────────────────────────────────────────────────────

def run(data_dir: Path, discipline: str = "ms") -> None:
    global _skipped_global
    _skipped_global = []
    disc = discipline.lower()

    # ── Load ──────────────────────────────────────────────────────────────
    try:
        matches, tournaments, players = _load_csvs(data_dir, discipline=disc)
    except FileNotFoundError as e:
        print(f"ERROR: Could not find CSV file: {e}", file=sys.stderr)
        print(f"  Run 'python run_pipeline.py --discipline {disc}' first to generate the data.",
              file=sys.stderr)
        sys.exit(1)

    nat_map = players.set_index("name")["nationality"].to_dict()

    # ── Compute ───────────────────────────────────────────────────────────
    appearances = build_match_appearances(matches, tournaments)
    par_df, replacement_level = compute_par(appearances)
    timeline_df = compute_timeline(appearances, replacement_level)

    # Join nationality
    par_df["nationality"] = par_df["player_name"].map(nat_map).fillna("")

    # Final column order for par_scores.csv
    par_df = par_df[[
        "player_name", "nationality", "matches_played",
        "avg_match_score", "par_score", "par_tier",
        "best_match_score", "best_win",
    ]].round({"avg_match_score": 4, "best_match_score": 4})

    # ── Save ──────────────────────────────────────────────────────────────
    out_scores   = data_dir / f"par_scores_{disc}.csv"
    out_timeline = data_dir / f"par_timeline_{disc}.csv"

    par_df.to_csv(out_scores, index=False)
    timeline_df.to_csv(out_timeline, index=False)

    print(f"  Saved {len(par_df)} player PAR scores → {out_scores}")
    print(f"  Saved {len(timeline_df)} timeline rows  → {out_timeline}")

    # ── Print summary ─────────────────────────────────────────────────────
    _print_results(par_df, replacement_level, _skipped_global, discipline=disc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute ShuttleIQ PAR scores.")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=_DATA_DIR,
        help=f"Root data directory (default: {_DATA_DIR})",
    )
    parser.add_argument(
        "--discipline",
        choices=["ms", "ws", "MS", "WS"],
        default="ms",
        help="Discipline to compute PAR for: ms or ws (default: ms)",
    )
    args = parser.parse_args()
    run(args.data_dir, discipline=args.discipline)


if __name__ == "__main__":
    main()
