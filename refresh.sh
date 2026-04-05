#!/bin/bash
set -e  # exit immediately on any error

# Always run from the shuttleiq project root regardless of where the script is called from
cd "$(dirname "$0")"

echo "🏸 ShuttleIQ Data Refresh Starting..."
echo "======================================"

# Step 1 — Scrape latest BWF data
echo ""
echo "📡 Step 1/3 — Scraping BWF World Tour data..."
python3 run_pipeline.py --years 2023 2024 2025 2026 --skip-if-cached

# Step 2 — Recalculate PAR scores
echo ""
echo "📊 Step 2/3 — Recalculating PAR scores..."
python3 model/par_calculator.py

# Step 3 — Push to GitHub
echo ""
echo "🚀 Step 3/3 — Pushing to GitHub..."
git add data/
git commit -m "data: refresh BWF data - $(date '+%B %Y')"
git push origin main

echo ""
echo "======================================"
echo "✅ Refresh complete! Site will update in ~2 mins."
echo "🌐 https://shuttleiq.com"
