"""
Rankings scraper for BWF Men's Singles world rankings.

Visits https://bwfworldtour.bwfbadminton.com/rankings/ and extracts the
current Men's Singles (MS) ranking table.

⚠️  IMPORTANT LIMITATION
The rankings page shows CURRENT rankings, not historical ones. For the PAR
formula you need rankings at the time of each match. Two workarounds:
  1. Run this scraper weekly and store snapshots in rankings_history.csv.
  2. Use tournament seedings as a proxy (seedings correlate strongly with
     world ranking at the time of entry).
For Phase 1 we store current rankings and flag missing historical data.
"""

import asyncio
import re
import logging
from dataclasses import dataclass
from playwright.async_api import async_playwright, Page

logger = logging.getLogger(__name__)

BASE_URL = "https://bwfworldtour.bwfbadminton.com"


@dataclass
class RankingEntry:
    rank: int
    name: str
    nationality: str   # 3-letter BWF country code
    points: Optional[float] = None
    bwf_id: Optional[int] = None
    bwf_url: str = ""


from typing import Optional


async def scrape_rankings(page: Page) -> list[RankingEntry]:
    """
    Scrape the current MS world rankings from the rankings page.
    Returns a list of RankingEntry objects sorted by rank.
    """
    url = f"{BASE_URL}/rankings/"
    logger.info("Scraping rankings: %s", url)

    await page.goto(url, wait_until="networkidle", timeout=45_000)
    await asyncio.sleep(3)

    # The rankings page has a category selector. We need to make sure
    # Men's Singles (MS) is selected. Look for a select element or tabs.
    try:
        # Try to find and click the MS category tab/button
        ms_selectors = [
            "text=Men's Singles",
            "text=MS",
            "[data-event='MS']",
            "a:has-text('Men')",
        ]
        for sel in ms_selectors:
            el = await page.query_selector(sel)
            if el:
                await el.click()
                await asyncio.sleep(2)
                break
    except Exception:
        pass  # Might already be on MS by default

    page_text = await page.inner_text("body")
    return _parse_rankings_text(page_text)


def _parse_rankings_text(text: str) -> list[RankingEntry]:
    """
    Parse the rankings table from the page text.
    Expected format per row (approximately):
        1   SHI Yu Qi   CHN   110750
        2   Viktor AXELSEN   DEN   99855
    """
    entries: list[RankingEntry] = []
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    # Find the start of the rankings table
    table_started = False
    rank_counter = 0

    for i, line in enumerate(lines):
        # Look for a line that starts with "1" followed by a player name
        if not table_started:
            if re.match(r"^1\s*$|^1\b", line) and i + 1 < len(lines):
                table_started = True

        if not table_started:
            # Also try: line is just a number
            if re.match(r"^\d+$", line) and int(line) == rank_counter + 1:
                table_started = True

        if table_started:
            # Try to parse: rank line followed by name and country
            if re.match(r"^\d+$", line):
                rank = int(line)
                # Expect: next line = name, then country, then points
                if i + 1 < len(lines) and re.search(r"[A-Za-zÀ-ÿ]", lines[i+1]):
                    name = lines[i+1]
                    nationality = ""
                    points = None
                    bwf_id = None
                    bwf_url = ""

                    if i + 2 < len(lines) and re.match(r"^[A-Z]{2,3}$", lines[i+2]):
                        nationality = lines[i+2]
                    if i + 3 < len(lines) and re.match(r"^\d[\d,\.]*$", lines[i+3].replace(",", "")):
                        try:
                            points = float(lines[i+3].replace(",", ""))
                        except ValueError:
                            pass

                    entries.append(RankingEntry(
                        rank=rank,
                        name=name,
                        nationality=nationality,
                        points=points,
                        bwf_id=bwf_id,
                        bwf_url=bwf_url,
                    ))
                    rank_counter = rank

            # Stop when we've gone far past rank 200 (avoid footer content)
            if rank_counter > 250:
                break

    # Fallback: use regex to find rank + name patterns
    if not entries:
        pattern = re.compile(
            r"(\d+)\s+([A-Z][A-Za-zÀ-ÿ'\s\-\.]+?)\s+([A-Z]{2,3})\s+([\d,]+)"
        )
        for m in pattern.finditer(text):
            rank = int(m.group(1))
            if rank > 300:
                continue
            try:
                points = float(m.group(4).replace(",", ""))
            except ValueError:
                points = None
            entries.append(RankingEntry(
                rank=rank,
                name=m.group(2).strip(),
                nationality=m.group(3).strip(),
                points=points,
            ))

    entries.sort(key=lambda e: e.rank)
    logger.info("Parsed %d ranking entries", len(entries))
    return entries


async def get_current_rankings() -> list[RankingEntry]:
    """Convenience function — launches browser, scrapes, closes."""
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
        rankings = await scrape_rankings(page)
        await browser.close()
    return rankings


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    async def main():
        rankings = await get_current_rankings()
        print(f"\nTop 10 MS Rankings:")
        for r in rankings[:10]:
            print(f"  {r.rank:>3}. {r.name:<30} {r.nationality}  {r.points}")

    asyncio.run(main())
