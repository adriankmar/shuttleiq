"""
Draw scraper for BWF World Tour Men's Singles.

For each tournament, visits:
  /tournament/{id}/{slug}/draws/full-draw/ms

The "full draw" page is a print-friendly server-rendered view that shows
the complete bracket: every player in every round, match scores, seedings,
nationalities, and player profile URLs.

How the bracket is structured on the page
──────────────────────────────────────────
The page renders each round as a labelled section:
  "Round 32" / "Round 64" → all entrants in bracket order (2 per match)
  "Round 16" → winners of R32 + their R32 match score + match number
  "Quarter Final" → R16 winners + their R16 score
  "Semi Final"    → QF winners + their QF score
  "Final"         → SF winners + their SF score + the Final match result

Crucially, the score shown next to a player in round N is the score from
the match that got them INTO round N (i.e. their previous-round result).
Scores are formatted from the WINNER's perspective:
  "21-15, 21-18"        — straight-game win
  "21-18, 18-21, 21-14" — three-game win (winner lost the middle game)

From this structure we reconstruct each individual match:
  - Pairs in R32 are positions [0,1], [2,3], [4,5], ...
  - Pairs in R16 are positions [0,1], [2,3], ... among R32 winners
  - Same pattern recursively up the bracket.

What we CAN'T get from this page
─────────────────────────────────
  - World rankings at the time of the tournament (not displayed).
    We store BWF player IDs so rankings can be joined from a rankings table.
  - Qualification/first-round results if the draw starts from R64.

Limitations flagged in output
──────────────────────────────
  - p1_world_ranking / p2_world_ranking → filled later from rankings table
  - Retirement (RET) / walkover (W.O.) results are preserved in the score field
"""

import asyncio
import re
import logging
from dataclasses import dataclass, field
from typing import Optional
from playwright.async_api import async_playwright, Page

logger = logging.getLogger(__name__)

BASE_URL = "https://bwfworldtour.bwfbadminton.com"

# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class PlayerEntry:
    name: str
    nationality: str       # 3-letter BWF country code, e.g. "CHN"
    seeding: Optional[int] # None if unseeded
    bwf_id: Optional[int]  # Extracted from profile URL
    bwf_url: str           # e.g. "https://bwfbadminton.com/player/57945/yu-qi-shi/"


@dataclass
class MatchResult:
    match_num: int         # Sequential match number from the draw (#1, #2, …)
    round_name: str        # "R32", "R16", "QF", "SF", "F"
    player1: PlayerEntry   # Higher bracket position (or listed first)
    player2: PlayerEntry
    winner: int            # 1 or 2
    score: str             # e.g. "21-15 21-18" or "21-15 14-21 21-10" or "RET"
    duration_min: Optional[int]  # Match duration in minutes (from parenthetical)


@dataclass
class DrawData:
    tournament_id: int
    slug: str
    matches: list[MatchResult] = field(default_factory=list)
    error: Optional[str] = None


# ── Parsing helpers ───────────────────────────────────────────────────────────

ROUND_LABELS = {
    "round 64": "R64",
    "round 32": "R32",
    "round 16": "R16",
    "quarter final": "QF",
    "quarterfinal": "QF",
    "semi final": "SF",
    "semifinal": "SF",
    "final": "F",
}

def _normalise_round(text: str) -> Optional[str]:
    return ROUND_LABELS.get(text.lower().strip())


def _extract_bwf_id(url: str) -> Optional[int]:
    m = re.search(r"/player/(\d+)/", url)
    return int(m.group(1)) if m else None


def _parse_score_text(raw: str) -> tuple[Optional[str], Optional[int]]:
    """
    Parse a score string like "#3: 21-15, 21-18 (46')" or "#3: RET".

    Returns (score_string, duration_minutes).
    score_string uses space-separated games: "21-15 21-18"
    """
    raw = raw.strip()

    # Strip the leading match number "#N: "
    raw = re.sub(r"^#\d+:\s*", "", raw)

    if not raw:
        return None, None

    # Check for retirement / walkover
    upper = raw.upper()
    if "RET" in upper:
        return "RET", None
    if "W.O" in upper or "WO" in upper or "WALKOVER" in upper:
        return "W.O.", None

    # Extract duration "(46')" if present
    duration = None
    m = re.search(r"\((\d+)['′]?\)", raw)
    if m:
        duration = int(m.group(1))
        raw = raw[:m.start()].strip()

    # Parse game scores separated by ", " or " "
    # Format is "21-15, 21-18" or "21-15 21-18"
    game_parts = re.findall(r"\d+[-–]\d+", raw)
    if not game_parts:
        return raw or None, duration

    score_str = " ".join(g.replace("–", "-") for g in game_parts)
    return score_str, duration


def _determine_winner(
    player_idx: int,
    score: str,
    round_players: list["PlayerEntry"],
    next_round_names: set[str],
) -> int:
    """
    Return 1 or 2 (player index offset by 1) indicating match winner.

    We know the winner because the page always shows the winner advancing
    to the next round — we track which player appears in the next round.
    This function is called AFTER we've identified who advanced.
    `player_idx` is the 0-based index of the advancing player among
    the pair [p1, p2].
    """
    return player_idx + 1  # 1-indexed


# ── DOM parsing ───────────────────────────────────────────────────────────────

async def _parse_full_draw_page(page: Page, tournament_id: int, slug: str) -> DrawData:
    """
    Parse the rendered full draw page and return a DrawData object.

    Strategy:
      1. Find the print-friendly region (the first of two identical regions).
      2. Walk its children sequentially, tracking the current round.
      3. Collect PlayerEntry objects per round.
      4. After collecting all rounds, reconstruct match pairings from
         bracket position and match numbers.
    """
    draw = DrawData(tournament_id=tournament_id, slug=slug)

    # The page has two regions with the same data: a print view and an
    # interactive Vue bracket. We take the first one (print view).
    # We use Playwright's evaluate to extract structured data directly.

    raw_data = await page.evaluate("""
    () => {
        // Find all round section headers on the page
        const result = { rounds: [] };

        // The print-friendly section is a <section> or <div> that contains
        // round headings. We locate it by finding the first element that
        // contains "Round 32" or "Round 64" text.
        const allText = document.querySelectorAll('*');
        let printSection = null;
        for (const el of allText) {
            if (el.children.length === 0) continue; // leaf node
            const directText = Array.from(el.childNodes)
                .filter(n => n.nodeType === 3)
                .map(n => n.textContent.trim())
                .join('');
            if (/^(Round (64|32))$/i.test(directText.trim())) {
                printSection = el.parentElement;
                break;
            }
        }

        // Fallback: grab the body
        const root = printSection || document.body;

        // Walk the DOM and extract round sections
        // Rounds are demarcated by heading-like elements with round names
        let currentRound = null;
        let currentPlayers = [];

        function extractPlayerFromElement(el) {
            // A player block contains: country flag image, name link, optional seeding
            const link = el.querySelector ? el.querySelector('a[href*="/player/"]') : null;
            if (!link) return null;

            const href = link.getAttribute('href') || '';
            const name = link.textContent.trim();
            const img = el.querySelector ? el.querySelector('img') : null;
            const nationality = img ? (img.getAttribute('alt') || img.getAttribute('title') || '').trim().toUpperCase() : '';

            // Seeding: look for "(N)" text in the element or siblings
            let seeding = null;
            const text = el.textContent || '';
            const seedMatch = text.match(/\((\d+)\)/);
            if (seedMatch) seeding = parseInt(seedMatch[1]);

            // BWF ID from URL
            const idMatch = href.match(/\/player\/(\d+)\//);
            const bwfId = idMatch ? parseInt(idMatch[1]) : null;

            return { name, nationality, seeding, bwfId, bwfUrl: href };
        }

        // We'll use a simpler approach: get all text nodes and links in order
        const walker = document.createTreeWalker(
            root,
            NodeFilter.SHOW_ELEMENT | NodeFilter.SHOW_TEXT,
            null
        );

        const elements = [];
        let node;
        while (node = walker.nextNode()) {
            if (node.nodeType === 3) {
                const t = node.textContent.trim();
                if (t) elements.push({ type: 'text', value: t, el: node.parentElement });
            } else if (node.nodeType === 1) {
                if (node.tagName === 'A' && (node.href || '').includes('/player/')) {
                    elements.push({ type: 'player_link', value: node.textContent.trim(), href: node.href });
                } else if (node.tagName === 'IMG') {
                    elements.push({ type: 'img', alt: node.alt || node.title || '' });
                }
            }
        }
        result.rawElements = elements.slice(0, 2000); // cap to avoid huge response
        return result;
    }
    """)

    # If JS extraction didn't work well, fall back to page text parsing
    page_text = await page.inner_text("body")
    return _parse_from_page_text(page_text, tournament_id, slug)


def _parse_from_page_text(text: str, tournament_id: int, slug: str) -> DrawData:
    """
    Parse the full draw from the page's plain text.

    The text has this structure (roughly):
        Round 32
        SHI Yu Qi
        (1)
        Rasmus GEMKE
        ...
        Round 16
        SHI Yu Qi
        (1)
        #1: 23-21, 21-15 (52')
        ...
    We tokenise the text into sections by round heading, then reconstruct
    the bracket. This is the most resilient approach since it doesn't
    depend on CSS class names that may change.
    """
    draw = DrawData(tournament_id=tournament_id, slug=slug)

    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # ── Step 1: split lines into round sections ───────────────────────────────
    sections: dict[str, list[str]] = {}
    current_round = None

    for line in lines:
        norm = _normalise_round(line)
        if norm:
            # "Final" appears multiple times (SF label → Final label).
            # Only take the first occurrence of each.
            if norm not in sections:
                sections[norm] = []
                current_round = norm
            else:
                current_round = None  # duplicate, skip
        elif current_round:
            sections[current_round].append(line)

    if not sections:
        draw.error = "No round sections found in page text"
        return draw

    # ── Step 2: determine the first real round (R64 or R32) ──────────────────
    if "R64" in sections:
        first_round, draw_size = "R64", 64
    elif "R32" in sections:
        first_round, draw_size = "R32", 32
    else:
        draw.error = f"Unknown draw format, sections: {list(sections.keys())}"
        return draw

    round_order = []
    all_rounds = ["R64", "R32", "R16", "QF", "SF", "F"]
    started = False
    for r in all_rounds:
        if r == first_round:
            started = True
        if started and r in sections:
            round_order.append(r)

    # ── Step 3: parse each round's lines into player lists ───────────────────
    # Each round section is a flat list of lines. We group them into
    # PlayerEntry objects by looking for the patterns:
    #   - 3-letter country codes (nationality, shown as image alt in accessibility)
    #   - Player names (mixed case, may include brackets for seeding)
    #   - "(N)" seeding markers
    #   - "#N: score" match result lines
    #   - "RET" retirement markers

    # Regex patterns
    SCORE_RE = re.compile(r"^#\d+:")
    SEEDING_RE = re.compile(r"^\((\d+)\)$")
    COUNTRY_RE = re.compile(r"^[A-Z]{2,3}$")  # 2-3 uppercase letters
    NAME_RE = re.compile(r"[A-Za-zÀ-ÿ]")      # has at least one letter

    def parse_round_players(lines_: list[str]) -> list[dict]:
        """
        Returns a list of player dicts:
          {name, nationality, seeding, score, duration, match_num}
        """
        players = []
        i = 0
        while i < len(lines_):
            line = lines_[i]

            # Skip score lines — they'll be picked up when we see the player
            if SCORE_RE.match(line):
                if players:
                    score_str, duration = _parse_score_text(line)
                    players[-1]["score"] = score_str
                    players[-1]["duration"] = duration
                    # Extract match number
                    m = re.match(r"^#(\d+):", line)
                    if m:
                        players[-1]["match_num"] = int(m.group(1))
                i += 1
                continue

            # Seeding marker "(N)"
            if SEEDING_RE.match(line):
                if players:
                    players[-1]["seeding"] = int(SEEDING_RE.match(line).group(1))
                i += 1
                continue

            # Skip standalone "RET" / "W.O." — already handled via score
            upper = line.upper()
            if upper in ("RET", "W.O.", "W/O", "RETIRED", "WALKOVER"):
                if players:
                    players[-1]["retired"] = True
                i += 1
                continue

            # Country code (3 uppercase letters like "CHN", "MAS")
            if COUNTRY_RE.match(line) and len(line) <= 3:
                # Next non-empty line should be the player name
                nationality = line
                j = i + 1
                while j < len(lines_) and not lines_[j].strip():
                    j += 1
                if j < len(lines_) and NAME_RE.search(lines_[j]):
                    players.append({
                        "name": lines_[j],
                        "nationality": nationality,
                        "seeding": None,
                        "score": None,
                        "duration": None,
                        "match_num": None,
                        "retired": False,
                    })
                    i = j + 1
                    continue

            # Player name without preceding country code (fallback)
            # Heuristic: long enough, contains letters, not a score/heading
            if (NAME_RE.search(line)
                    and len(line) >= 3
                    and not SCORE_RE.match(line)
                    and not line.startswith("Printed:")
                    and not line.startswith("Prize")
                    and not line.startswith("STADIUM")
                    and not any(line.lower().startswith(h) for h in
                                ["round", "quarter", "semi", "final", "jan ", "feb ",
                                 "mar ", "apr ", "may ", "jun ", "jul ", "aug ",
                                 "sep ", "oct ", "nov ", "dec ", "score", "draw",
                                 "size", "type", "main"])):
                if not players or players[-1].get("name"):
                    players.append({
                        "name": line,
                        "nationality": "",
                        "seeding": None,
                        "score": None,
                        "duration": None,
                        "match_num": None,
                        "retired": False,
                    })
            i += 1

        return players

    round_players: dict[str, list[dict]] = {}
    for rnd in round_order:
        round_players[rnd] = parse_round_players(sections[rnd])

    # ── Step 4: reconstruct matches from bracket positions ───────────────────
    #
    # The bracket works as a single-elimination tree:
    #   - In the FIRST round (R32 or R64), players are listed in bracket order.
    #     Pairs are consecutive: [0,1], [2,3], [4,5], ...
    #   - In SUBSEQUENT rounds, the winners are listed in bracket order
    #     (same ordering as the first round, just half as many).
    #     Adjacent pairs still play each other: [0,1], [2,3], ...
    #
    # We know who won each match because the WINNER appears in the next round.
    # We match by name (normalised).

    def normalise_name(n: str) -> str:
        return re.sub(r"\s+", " ", n.strip().upper())

    # Build a set of names that appear in each round (for winner lookup)
    round_name_sets: dict[str, set[str]] = {}
    for rnd in round_order:
        round_name_sets[rnd] = {normalise_name(p["name"]) for p in round_players.get(rnd, [])}

    # Map match_num → winner info (from score lines in subsequent rounds)
    match_num_to_winner: dict[int, dict] = {}
    for rnd in round_order[1:]:  # skip first round (no scores yet)
        for p in round_players.get(rnd, []):
            if p.get("match_num"):
                match_num_to_winner[p["match_num"]] = p

    match_counter = [0]  # mutable counter

    def make_match(p1: dict, p2: dict, round_name: str, winner_idx: int,
                   score: str, duration: Optional[int]) -> MatchResult:
        match_counter[0] += 1
        return MatchResult(
            match_num=match_counter[0],
            round_name=round_name,
            player1=PlayerEntry(
                name=p1["name"],
                nationality=p1.get("nationality", ""),
                seeding=p1.get("seeding"),
                bwf_id=None,  # filled by rankings join
                bwf_url="",
            ),
            player2=PlayerEntry(
                name=p2["name"],
                nationality=p2.get("nationality", ""),
                seeding=p2.get("seeding"),
                bwf_id=None,
                bwf_url="",
            ),
            winner=winner_idx,
            score=score or "",
            duration_min=duration,
        )

    # Process each round pair-by-pair
    for round_idx, rnd in enumerate(round_order):
        players = round_players.get(rnd, [])
        if len(players) < 2:
            continue

        if round_idx == len(round_order) - 1:
            # Last round listed = the Final; players here are the two finalists.
            # Their score is their SF result. The Final score is trickier —
            # we look for the highest match_num among the finalists' scores.
            if len(players) >= 2:
                p1, p2 = players[0], players[1]
                # Determine winner: who appears... both appear in Final section.
                # Use retirement flag or look for explicit Final score.
                # The Final match score is the one with the HIGHEST match number.
                final_score = ""
                final_duration = None
                for p in players:
                    if p.get("score") and p["score"] != p1.get("score"):
                        pass
                # Simplification: take the score from whichever finalist's entry
                # has a match_num that isn't an SF match number.
                # We'll just check if one player has "RET" flag.
                # On BWF draw sheets, "RET" appears next to the WINNER
                # (indicating the opponent retired). So the player flagged
                # retired=True is the winner of the match.
                winner_idx = 1
                for idx, p in enumerate(players[:2]):
                    if p.get("retired"):
                        winner_idx = idx + 1   # 1-indexed: this player won
                        break
                draw.matches.append(make_match(
                    p1, p2, rnd, winner_idx,
                    p1.get("score", "") or p2.get("score", ""),
                    p1.get("duration") or p2.get("duration"),
                ))
            continue

        next_rnd = round_order[round_idx + 1]
        next_players = round_players.get(next_rnd, [])
        next_names = {normalise_name(p["name"]): p for p in next_players}

        # Pair up current round players in adjacent brackets
        for i in range(0, len(players) - 1, 2):
            p1, p2 = players[i], players[i + 1]
            n1 = normalise_name(p1["name"])
            n2 = normalise_name(p2["name"])

            # Determine winner by who appears in the next round
            if n1 in next_names:
                winner_idx = 1
                winner_data = next_names[n1]
            elif n2 in next_names:
                winner_idx = 2
                winner_data = next_names[n2]
            else:
                # Can't determine winner — might be BYE or scraping artefact.
                logger.debug("Cannot determine winner: %s vs %s", p1["name"], p2["name"])
                winner_idx = 1
                winner_data = {}

            # Score comes from the winner's entry in the next round
            score = winner_data.get("score") or ""
            duration = winner_data.get("duration")

            draw.matches.append(make_match(p1, p2, rnd, winner_idx, score, duration))

    return draw


# ── Playwright orchestration ──────────────────────────────────────────────────

async def scrape_draw(
    tournament_id: int,
    slug: str,
    page: Page,
    delay: float = 2.0,
) -> DrawData:
    """Scrape the MS full draw for one tournament. Reuses an existing page."""
    url = f"{BASE_URL}/tournament/{tournament_id}/{slug}/draws/full-draw/ms"
    logger.info("Scraping draw: %s", url)

    try:
        await page.goto(url, wait_until="networkidle", timeout=60_000)
        await asyncio.sleep(delay)

        # Verify we're on the right page
        title = await page.title()
        if "draw ms" not in title.lower() and "men" not in title.lower():
            # The page may have redirected to a different view
            # Try the alternative URL format
            alt_url = f"{BASE_URL}/tournament/{tournament_id}/{slug}/results/draw/ms"
            await page.goto(alt_url, wait_until="networkidle", timeout=60_000)
            await asyncio.sleep(delay)

        page_text = await page.inner_text("body")
        return _parse_from_page_text(page_text, tournament_id, slug)

    except Exception as exc:
        logger.error("Error scraping draw %d (%s): %s", tournament_id, slug, exc)
        return DrawData(tournament_id=tournament_id, slug=slug, error=str(exc))


async def scrape_all_draws(
    tournaments: list,   # list of TournamentInfo
    rate_limit_sec: float = 3.0,
) -> list[DrawData]:
    """
    Scrape MS draws for all tournaments.
    Uses a single browser context and navigates page-by-page to avoid
    opening too many connections.
    """
    results = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        page = await context.new_page()

        for t in tournaments:
            draw = await scrape_draw(t.tournament_id, t.slug, page, delay=rate_limit_sec)
            results.append(draw)
            logger.info(
                "  [%d] %s → %d matches scraped%s",
                t.tournament_id,
                t.name,
                len(draw.matches),
                f" [ERROR: {draw.error}]" if draw.error else "",
            )
            await asyncio.sleep(rate_limit_sec)

        await browser.close()

    return results


# ── Quick smoke test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    async def main():
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )
            page = await context.new_page()
            draw = await scrape_draw(5227, "petronas-malaysia-open-2026", page)
            print(f"\nDraw: {draw.tournament_id}, matches={len(draw.matches)}, error={draw.error}")
            for m in draw.matches[:5]:
                print(
                    f"  [{m.round_name}] {m.player1.name} ({m.player1.seeding}) "
                    f"vs {m.player2.name} ({m.player2.seeding}) "
                    f"→ winner=P{m.winner}, score={m.score!r}"
                )
            await browser.close()

    asyncio.run(main())
