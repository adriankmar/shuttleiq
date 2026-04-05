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
from typing import Optional
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


async def scrape_rankings(page: Page) -> list[RankingEntry]:
    """
    Scrape the current MS world rankings from the rankings page.

    Uses JavaScript DOM extraction instead of inner_text() because player
    names and nationalities are split across text nodes and flag images
    (img.alt), making plain text parsing unreliable.

    Returns a list of RankingEntry objects sorted by rank.
    """
    url = f"{BASE_URL}/rankings/"
    logger.info("Scraping rankings: %s", url)

    await page.goto(url, wait_until="networkidle", timeout=45_000)
    await asyncio.sleep(3)

    # Ensure Men's Singles (MS) is selected
    try:
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
        pass

    raw_entries = await page.evaluate("""
    () => {
        const entries = [];
        const seen = new Set();

        // ── Strategy 1: table rows containing player links ────────────────
        document.querySelectorAll('tr').forEach(row => {
            const link = row.querySelector('a[href*="/player/"]');
            if (!link) return;

            const name = link.textContent.trim();
            if (!name || name.length < 2 || seen.has(name)) return;

            // Nationality from flag image alt
            const img = row.querySelector('img');
            const nat = img ? (img.alt || img.title || '').trim().toUpperCase() : '';

            // Rank: first cell that is a plain integer 1-300
            const cells = Array.from(row.querySelectorAll('td, th'));
            let rank = null, points = null;
            for (const cell of cells) {
                const txt = cell.textContent.trim().replace(/,/g, '');
                if (rank === null && /^\d{1,3}$/.test(txt)) {
                    const n = parseInt(txt);
                    if (n >= 1 && n <= 300) rank = n;
                } else if (points === null && /^\d{5,}$/.test(txt)) {
                    points = parseFloat(txt);
                }
            }

            const href = link.href || link.getAttribute('href') || '';
            const idM  = href.match(/\/player\/(\d+)\//);

            if (name && rank) {
                seen.add(name);
                entries.push({
                    rank, name,
                    nationality: /^[A-Z]{2,3}$/.test(nat) ? nat : '',
                    points,
                    bwfId:  idM ? parseInt(idM[1]) : null,
                    bwfUrl: href,
                });
            }
        });

        // ── Strategy 2: list/div items (for non-table layouts) ───────────
        if (entries.length < 10) {
            document.querySelectorAll('a[href*="/player/"]').forEach(link => {
                const name = link.textContent.trim();
                if (!name || name.length < 2 || seen.has(name)) return;

                // Walk up to find a container with a rank number
                let container = link.parentElement;
                for (let depth = 0; depth < 5 && container; depth++) {
                    // Look for a sibling or child element that holds a rank
                    const rankEl = container.querySelector(
                        '[class*="rank"], [class*="position"], [class*="number"]'
                    );
                    const rankTxt = (rankEl || container).textContent
                        .trim().match(/^\s*(\d{1,3})\s/);
                    if (rankTxt) {
                        const rank = parseInt(rankTxt[1]);
                        if (rank >= 1 && rank <= 300) {
                            const img = container.querySelector('img');
                            const nat = img
                                ? (img.alt || img.title || '').trim().toUpperCase()
                                : '';
                            const href = link.href || '';
                            const idM  = href.match(/\/player\/(\d+)\//);
                            seen.add(name);
                            entries.push({
                                rank, name,
                                nationality: /^[A-Z]{2,3}$/.test(nat) ? nat : '',
                                points: null,
                                bwfId:  idM ? parseInt(idM[1]) : null,
                                bwfUrl: href,
                            });
                            break;
                        }
                    }
                    container = container.parentElement;
                }
            });
        }

        return entries;
    }
    """)

    entries: list[RankingEntry] = []
    seen_ranks: set[int] = set()
    for e in (raw_entries or []):
        rank = e.get("rank")
        if not rank or rank in seen_ranks:
            continue
        seen_ranks.add(rank)
        entries.append(RankingEntry(
            rank=rank,
            name=e.get("name", ""),
            nationality=e.get("nationality", ""),
            points=e.get("points"),
            bwf_id=e.get("bwfId"),
            bwf_url=e.get("bwfUrl", ""),
        ))

    entries.sort(key=lambda r: r.rank)
    logger.info("Parsed %d ranking entries via JS DOM", len(entries))

    # Fallback to text parsing if JS extraction returned nothing useful
    if len(entries) < 5:
        logger.warning("JS extraction returned < 5 entries; falling back to text parse")
        page_text = await page.inner_text("body")
        entries = _parse_rankings_text_fallback(page_text)

    return entries


def _parse_rankings_text_fallback(text: str) -> list[RankingEntry]:
    """
    Regex fallback for rankings parsing.
    Looks for patterns like "1  SHI Yu Qi  CHN  110750".
    """
    entries: list[RankingEntry] = []
    pattern = re.compile(
        r"(\d{1,3})\s+([A-Z][A-Za-zÀ-ÿ'\s\-\.]+?)\s+([A-Z]{2,3})\s+([\d,]+)"
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
    logger.info("Text-fallback parsed %d ranking entries", len(entries))
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
