"""
Calendar scraper for BWF World Tour.

Visits https://bwfworldtour.bwfbadminton.com/calendar/?cyear={year}
and extracts all World Tour tournament IDs, names, tiers, locations, and dates.

The site is WordPress-rendered HTML (not a pure SPA), so Playwright renders the
page and we parse the resulting DOM. Plain HTTP requests return 403.
"""

import asyncio
import re
import logging
from dataclasses import dataclass, field
from typing import Optional
from playwright.async_api import async_playwright, Page

logger = logging.getLogger(__name__)

BASE_URL = "https://bwfworldtour.bwfbadminton.com"

# Maps the tier text on the site to our canonical tier labels.
# "Finals" catches the year-end World Tour Finals event.
TIER_MAP = {
    "Super 1000": "Super1000",
    "Super 750":  "Super750",
    "Super 500":  "Super500",
    "Super 300":  "Super300",
    "Finals":     "Finals",
}

# Only collect tiers that are part of the World Tour proper.
WORLD_TOUR_TIERS = set(TIER_MAP.values())


@dataclass
class TournamentInfo:
    tournament_id: int
    name: str
    slug: str
    tier: str
    location: str
    date: str          # ISO start date, e.g. "2026-01-06"
    year: int
    draw_url: str = field(init=False)

    def __post_init__(self):
        self.draw_url = (
            f"{BASE_URL}/tournament/{self.tournament_id}/{self.slug}"
            f"/draws/full-draw/ms"
        )


def _parse_date(date_text: str, year: int) -> str:
    """
    Convert the calendar date strings to an ISO start date.

    Examples:
        "06 - 11 Jan"         -> "2026-01-06"
        "30 Jun - 05 Jul"     -> "2026-06-30"
        "27 Jan - 01 Feb"     -> "2026-01-27"
        "09 - 14 Dec"         -> "2026-12-09"
    """
    MONTHS = {
        "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
        "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
    }

    date_text = date_text.strip()

    # Pattern: "DD Mon - DD Mon"  (cross-month range)
    m = re.match(r"(\d+)\s+(\w+)\s*-\s*\d+\s+(\w+)", date_text)
    if m:
        day, month_str = int(m.group(1)), m.group(2)
        month = MONTHS.get(month_str[:3], 1)
        return f"{year}-{month:02d}-{day:02d}"

    # Pattern: "DD - DD Mon"  (same-month range)
    m = re.match(r"(\d+)\s*-\s*\d+\s+(\w+)", date_text)
    if m:
        day, month_str = int(m.group(1)), m.group(2)
        month = MONTHS.get(month_str[:3], 1)
        return f"{year}-{month:02d}-{day:02d}"

    logger.warning("Could not parse date: '%s'", date_text)
    return f"{year}-01-01"


def _parse_tier(tier_text: str) -> Optional[str]:
    """Return canonical tier string or None if not a World Tour event."""
    for key, value in TIER_MAP.items():
        if key.lower() in tier_text.lower():
            return value
    # Check the Finals shorthand used on the World Tour Finals site
    if "finals" in tier_text.lower():
        return "Finals"
    return None


def _extract_id_and_slug(href: str) -> Optional[tuple[int, str]]:
    """
    Pull tournament_id and slug from a URL like:
    /tournament/5227/petronas-malaysia-open-2026/results/
    """
    m = re.search(r"/tournament/(\d+)/([^/]+)/", href)
    if m:
        return int(m.group(1)), m.group(2)
    return None


async def _scrape_year(page: Page, year: int) -> list[TournamentInfo]:
    """Scrape all World Tour tournaments for a given year."""
    url = f"{BASE_URL}/calendar/?cyear={year}&rstate=all"
    logger.info("Fetching calendar for %d: %s", year, url)

    await page.goto(url, wait_until="networkidle", timeout=45_000)
    # Small extra wait for any late-loading JS
    await asyncio.sleep(2)

    tournaments: list[TournamentInfo] = []

    # Each tournament card is an <a> element linking to the tournament page.
    # We query all anchor tags that point to /tournament/ URLs.
    cards = await page.query_selector_all("a[href*='/tournament/']")

    seen_ids: set[int] = set()

    for card in cards:
        href = await card.get_attribute("href") or ""
        parsed = _extract_id_and_slug(href)
        if not parsed:
            continue
        tid, slug = parsed

        if tid in seen_ids:
            continue

        # Extract tier text from the card
        tier_el = await card.query_selector(
            "[class*='super'], [class*='tier'], [class*='category']"
        )
        tier_text = (await tier_el.inner_text()).strip() if tier_el else ""

        # Fallback: search all text in the card for a tier label
        if not tier_text:
            card_text = await card.inner_text()
            for key in TIER_MAP:
                if key.lower() in card_text.lower():
                    tier_text = key
                    break
            if not tier_text and "finals" in card_text.lower():
                tier_text = "Finals"

        tier = _parse_tier(tier_text)
        if tier not in WORLD_TOUR_TIERS:
            # Skip non-World-Tour events (e.g. World Championships, team events)
            continue

        # Extract name
        name_el = await card.query_selector(
            "[class*='title'], [class*='name'], h2, h3, h4"
        )
        name = (await name_el.inner_text()).strip() if name_el else slug.replace("-", " ").title()

        # Fallback name from slug
        if not name or len(name) < 4:
            name = slug.replace("-", " ").title()

        # Extract date
        date_el = await card.query_selector("[class*='date'], [class*='time']")
        date_text = (await date_el.inner_text()).strip() if date_el else ""
        date_iso = _parse_date(date_text, year) if date_text else f"{year}-01-01"

        # Extract location
        loc_el = await card.query_selector("[class*='location'], [class*='venue'], [class*='city']")
        location = (await loc_el.inner_text()).strip() if loc_el else ""

        seen_ids.add(tid)
        tournaments.append(TournamentInfo(
            tournament_id=tid,
            name=name,
            slug=slug,
            tier=tier,
            location=location,
            date=date_iso,
            year=year,
        ))
        logger.info("  Found: [%d] %s (%s) on %s", tid, name, tier, date_iso)

    return tournaments


async def scrape_calendar(years: list[int]) -> list[TournamentInfo]:
    """
    Scrape the BWF World Tour calendar for each year in `years`.
    Returns a deduplicated list of TournamentInfo objects.
    """
    all_tournaments: list[TournamentInfo] = []

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

        for year in years:
            try:
                results = await _scrape_year(page, year)
                all_tournaments.extend(results)
                logger.info("Year %d: found %d tournaments", year, len(results))
            except Exception as exc:
                logger.error("Error scraping calendar for %d: %s", year, exc)

        await browser.close()

    return all_tournaments


# ── quick smoke-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    async def main():
        tournaments = await scrape_calendar([2024, 2025])
        print(f"\nFound {len(tournaments)} tournaments:")
        for t in tournaments[:5]:
            print(f"  [{t.tournament_id}] {t.name} | {t.tier} | {t.date} | {t.location}")

    asyncio.run(main())
