"""
ShuttleIQ — BWF Men's Singles Analytics Dashboard
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ShuttleIQ",
    page_icon="🏸",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
PROC_DIR = DATA_DIR / "processed"

# ── Constants ─────────────────────────────────────────────────────────────────
FLAG_MAP = {
    "CHN": "🇨🇳", "THA": "🇹🇭", "INA": "🇮🇩", "MAS": "🇲🇾", "JPN": "🇯🇵",
    "DEN": "🇩🇰", "IND": "🇮🇳", "KOR": "🇰🇷", "TPE": "🇹🇼", "HKG": "🇭🇰",
    "SGP": "🇸🇬", "FRA": "🇫🇷", "GBR": "🇬🇧", "ENG": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "IRL": "🇮🇪",
    "CAN": "🇨🇦", "AUS": "🇦🇺", "NZL": "🇳🇿", "USA": "🇺🇸", "GER": "🇩🇪",
    "NED": "🇳🇱", "ESP": "🇪🇸", "SUI": "🇨🇭", "FIN": "🇫🇮", "BEL": "🇧🇪",
    "POL": "🇵🇱", "ITA": "🇮🇹", "CRO": "🇭🇷", "UKR": "🇺🇦", "VIE": "🇻🇳",
    "SRI": "🇱🇰", "MAC": "🇲🇴", "KAZ": "🇰🇿", "MEX": "🇲🇽", "BRA": "🇧🇷",
    "AUT": "🇦🇹", "ISR": "🇮🇱", "AZE": "🇦🇿", "ESA": "🇸🇻", "GUA": "🇬🇹",
}

TIER_COLORS = {
    "Elite":         "#FFD700",
    "Above Average": "#2ECC71",
    "Average":       "#3498DB",
    "Below Average": "#95A5A6",
}

TIER_ORDER = ["Elite", "Above Average", "Average", "Below Average"]


# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data
def load_data(discipline: str = "ms"):
    disc = discipline.lower()
    par_scores   = pd.read_csv(DATA_DIR / f"par_scores_{disc}.csv")
    par_timeline = pd.read_csv(DATA_DIR / f"par_timeline_{disc}.csv")
    tournaments  = pd.read_csv(PROC_DIR / "tournaments.csv")
    players      = pd.read_csv(PROC_DIR / f"players_{disc}.csv")

    # Parse dates
    par_timeline["date"] = pd.to_datetime(par_timeline["date"])
    tournaments["date"]  = pd.to_datetime(tournaments["date"])

    # Merge win_rate into par_scores
    par_scores = par_scores.merge(
        players[["name", "win_rate"]].rename(columns={"name": "player_name"}),
        on="player_name",
        how="left",
    )

    return par_scores, par_timeline, tournaments, players


# ── Helpers ───────────────────────────────────────────────────────────────────
def flag(nat: str) -> str:
    return FLAG_MAP.get(str(nat).upper(), "🏳")


def tier_badge(tier: str) -> str:
    color = TIER_COLORS.get(tier, "#95A5A6")
    return (
        f'<span style="background:{color};color:#fff;padding:2px 10px;'
        f'border-radius:12px;font-size:0.78rem;font-weight:600;">{tier}</span>'
    )


def fmt_par(v) -> str:
    try:
        return f"{float(v):+.3f}"
    except (TypeError, ValueError):
        return "—"


def fmt_pct(v) -> str:
    try:
        return f"{float(v)*100:.1f}%"
    except (TypeError, ValueError):
        return "—"


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🏸 ShuttleIQ")
    st.caption("BWF World Tour Analytics")
    st.divider()

    discipline_label = st.radio(
        "Discipline",
        ["👨 Men's Singles", "👩 Women's Singles"],
        index=0,
    )
    discipline = "ms" if "Men" in discipline_label else "ws"

    st.divider()
    page = st.radio(
        "Navigate",
        ["🏆 Leaderboard", "👤 Player Profile", "⚔️ Head to Head"],
        label_visibility="collapsed",
    )
    st.divider()

par_scores, par_timeline, tournaments, players = load_data(discipline)

with st.sidebar:
    st.caption(f"📊 {len(par_scores)} players · {len(par_timeline)} tournament entries")
    st.caption("Data: BWF World Tour 2023–2026")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — LEADERBOARD
# ══════════════════════════════════════════════════════════════════════════════
disc_label = "Men's Singles" if discipline == "ms" else "Women's Singles"

if page == "🏆 Leaderboard":
    st.title(f"🏆 ShuttleIQ — BWF {disc_label} Player Rankings")
    st.caption(f"Performance Above Replacement (PAR) rankings based on 2023–2026 BWF World Tour {disc_label} data")

    # ── Filters ───────────────────────────────────────────────────────────────
    col_search, col_tier, col_nat = st.columns([3, 2, 2])
    with col_search:
        search = st.text_input("🔍 Search player or nationality", placeholder="e.g. SHI Yu Qi or CHN")
    with col_tier:
        tier_filter = st.multiselect(
            "Filter by tier",
            TIER_ORDER,
            default=TIER_ORDER,
        )
    with col_nat:
        nat_options = sorted(par_scores["nationality"].dropna().unique())
        nat_filter = st.multiselect("Filter by nationality", nat_options)

    # ── Apply filters ─────────────────────────────────────────────────────────
    df_all = par_scores.copy().sort_values("par_score", ascending=False).reset_index(drop=True)

    # Split into qualified (≥15 matches) and emerging (<15 matches)
    MIN_MATCHES = 15
    df_qualified = df_all[df_all["matches_played"] >= MIN_MATCHES].copy().reset_index(drop=True)
    df_emerging  = df_all[df_all["matches_played"] <  MIN_MATCHES].copy().reset_index(drop=True)
    df_qualified["rank"] = df_qualified.index + 1

    # Apply search / tier / nat filters to qualified
    df = df_qualified.copy()
    if search:
        mask = (
            df["player_name"].str.contains(search, case=False, na=False)
            | df["nationality"].str.contains(search, case=False, na=False)
        )
        df = df[mask]
    if tier_filter:
        df = df[df["par_tier"].isin(tier_filter)]
    if nat_filter:
        df = df[df["nationality"].isin(nat_filter)]

    st.markdown(f"**{len(df)} qualified players shown** (min. {MIN_MATCHES} matches)")
    st.divider()

    def _render_table_rows(rows_df, show_rank=True):
        hcols = st.columns([0.5, 3, 1, 1.2, 1.8, 1, 1])
        for col, label in zip(hcols, ["Rank", "Player", "Nat", "PAR Score", "Tier", "Matches", "Win Rate"]):
            col.markdown(f"**{label}**")
        st.divider()
        for _, row in rows_df.iterrows():
            cols = st.columns([0.5, 3, 1, 1.2, 1.8, 1, 1])
            if show_rank and "rank" in row:
                cols[0].markdown(f"**#{int(row['rank'])}**")
            else:
                cols[0].markdown("—")
            cols[1].markdown(f"{row['player_name']}")
            cols[2].markdown(f"{flag(row['nationality'])} {row['nationality']}")
            par_val = float(row["par_score"]) if pd.notna(row["par_score"]) else 0
            color = "#2ECC71" if par_val > 0 else "#E74C3C" if par_val < 0 else "#95A5A6"
            cols[3].markdown(
                f'<span style="font-weight:700;color:{color};">{fmt_par(row["par_score"])}</span>',
                unsafe_allow_html=True,
            )
            cols[4].markdown(tier_badge(row["par_tier"]), unsafe_allow_html=True)
            cols[5].markdown(str(int(row["matches_played"])))
            cols[6].markdown(fmt_pct(row.get("win_rate")))

    # ── Main ranked table ─────────────────────────────────────────────────────
    _render_table_rows(df, show_rank=True)

    # ── Emerging Players (collapsible) ────────────────────────────────────────
    if not df_emerging.empty:
        # Apply same search/nat filters to emerging section
        de = df_emerging.copy()
        if search:
            mask = (
                de["player_name"].str.contains(search, case=False, na=False)
                | de["nationality"].str.contains(search, case=False, na=False)
            )
            de = de[mask]
        if nat_filter:
            de = de[de["nationality"].isin(nat_filter)]

        if not de.empty:
            st.divider()
            with st.expander(f"📈 Emerging Players ({len(de)}) — Min. {MIN_MATCHES} matches required for full ranking"):
                st.caption(f"These players have fewer than {MIN_MATCHES} matches and are not included in the main rankings.")
                _render_table_rows(de, show_rank=False)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — PLAYER PROFILE
# ══════════════════════════════════════════════════════════════════════════════
elif page == "👤 Player Profile":
    st.title(f"👤 Player Profile — {disc_label}")

    all_players = sorted(par_scores["player_name"].dropna().unique())
    default_idx = all_players.index("SHI Yu Qi") if "SHI Yu Qi" in all_players else 0

    selected = st.selectbox("Select player", all_players, index=default_idx)

    row = par_scores[par_scores["player_name"] == selected].iloc[0]
    timeline = par_timeline[par_timeline["player_name"] == selected].sort_values("date")

    st.divider()

    # ── Hero section ──────────────────────────────────────────────────────────
    hero_l, hero_m, hero_r = st.columns([1, 2, 1])
    with hero_l:
        nat = str(row["nationality"]) if pd.notna(row["nationality"]) else ""
        st.markdown(
            f'<div style="font-size:3rem;line-height:1">{flag(nat)}</div>',
            unsafe_allow_html=True,
        )
        st.markdown(f"**{nat}**")
    with hero_m:
        st.markdown(f"## {selected}")
        st.markdown(tier_badge(row["par_tier"]), unsafe_allow_html=True)
    with hero_r:
        par_val = float(row["par_score"]) if pd.notna(row["par_score"]) else 0
        color = "#2ECC71" if par_val > 0 else "#E74C3C"
        st.markdown(
            f'<div style="font-size:2.5rem;font-weight:800;color:{color};">{fmt_par(row["par_score"])}</div>',
            unsafe_allow_html=True,
        )
        st.caption("PAR Score")

    st.divider()

    # ── Metric cards ──────────────────────────────────────────────────────────
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Matches Played", int(row["matches_played"]))

    win_rate_val = row.get("win_rate")
    m2.metric("Win Rate", fmt_pct(win_rate_val))

    bms = row.get("best_match_score")
    m3.metric("Best Match Score", f"{float(bms):.3f}" if pd.notna(bms) else "—")

    bw = row.get("best_win")
    m4.markdown("**Best Win**")
    m4.markdown(str(bw) if pd.notna(bw) else "—")

    st.divider()

    # ── Career Arc chart ──────────────────────────────────────────────────────
    st.subheader("Career Arc")

    if timeline.empty:
        st.info("No tournament data available for this player.")
    else:
        tier_color_map = {
            "Super1000": "#E74C3C",
            "Super750":  "#E67E22",
            "Super500":  "#F1C40F",
            "Super300":  "#2ECC71",
            "Finals":    "#9B59B6",
        }

        fig = go.Figure()

        # Shaded zero line
        fig.add_hline(y=0, line_dash="dash", line_color="#BDC3C7", line_width=1)

        # PAR line
        fig.add_trace(go.Scatter(
            x=timeline["date"],
            y=timeline["tournament_par"],
            mode="lines+markers",
            line=dict(color="#3498DB", width=2.5),
            marker=dict(
                size=9,
                color=[tier_color_map.get(t, "#95A5A6") for t in timeline["tier"]],
                line=dict(color="white", width=1.5),
            ),
            customdata=list(zip(timeline["tournament_name"], timeline["tier"], timeline["matches_in_tournament"])),
            hovertemplate=(
                "<b>%{customdata[0]}</b><br>"
                "Tier: %{customdata[1]}<br>"
                "PAR: %{y:.3f}<br>"
                "Matches: %{customdata[2]}<br>"
                "Date: %{x|%b %Y}<extra></extra>"
            ),
            name="Tournament PAR",
        ))

        # Rolling average (5-tournament window)
        if len(timeline) >= 3:
            roll = timeline["tournament_par"].rolling(window=5, min_periods=1).mean()
            fig.add_trace(go.Scatter(
                x=timeline["date"],
                y=roll,
                mode="lines",
                line=dict(color="#E74C3C", width=1.5, dash="dot"),
                name="5-tournament avg",
                hoverinfo="skip",
            ))

        fig.update_layout(
            height=380,
            margin=dict(l=20, r=20, t=20, b=20),
            xaxis=dict(title="", showgrid=False, color="white", tickfont=dict(color="white")),
            yaxis=dict(title="PAR Score", zeroline=False, gridcolor="rgba(255,255,255,0.1)", color="white", tickfont=dict(color="white"), titlefont=dict(color="white")),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font=dict(color="white"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color="white")),
            hovermode="closest",
        )

        st.plotly_chart(fig, use_container_width=True)

        # Legend: dot colours = tier
        legend_cols = st.columns(len(tier_color_map))
        for col, (tier_name, col_hex) in zip(legend_cols, tier_color_map.items()):
            col.markdown(
                f'<span style="color:{col_hex};font-size:1.2rem;">●</span> {tier_name}',
                unsafe_allow_html=True,
            )

    st.divider()

    # ── Tournament results table ──────────────────────────────────────────────
    st.subheader("Tournament Results")
    if not timeline.empty:
        display = timeline[["date", "tournament_name", "tier", "matches_in_tournament", "tournament_par"]].copy()
        display["date"] = display["date"].dt.strftime("%Y-%m-%d")
        display["tournament_par"] = display["tournament_par"].round(3)
        display = display.sort_values("date", ascending=False)
        display.columns = ["Date", "Tournament", "Tier", "Matches", "PAR"]
        st.dataframe(display, use_container_width=True, hide_index=True)
    else:
        st.info("No tournament results to display.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — HEAD TO HEAD
# ══════════════════════════════════════════════════════════════════════════════
elif page == "⚔️ Head to Head":
    st.title("⚔️ Head to Head Comparison")

    all_players = sorted(par_scores["player_name"].dropna().unique())

    p1_default = all_players.index("SHI Yu Qi") if "SHI Yu Qi" in all_players else 0
    p2_default = all_players.index("Kunlavut VITIDSARN") if "Kunlavut VITIDSARN" in all_players else 1

    col_p1, col_p2 = st.columns(2)
    with col_p1:
        player1 = st.selectbox("Player 1", all_players, index=p1_default)
    with col_p2:
        player2 = st.selectbox("Player 2", all_players, index=p2_default)

    if player1 == player2:
        st.warning("Select two different players to compare.")
        st.stop()

    r1 = par_scores[par_scores["player_name"] == player1].iloc[0]
    r2 = par_scores[par_scores["player_name"] == player2].iloc[0]

    st.divider()

    # ── Side-by-side stat cards ───────────────────────────────────────────────
    stat_labels = ["PAR Score", "Tier", "Matches Played", "Win Rate", "Avg Match Score", "Best Win"]

    def get_stat(row, label):
        if label == "PAR Score":    return fmt_par(row["par_score"])
        if label == "Tier":         return row["par_tier"]
        if label == "Matches Played": return str(int(row["matches_played"]))
        if label == "Win Rate":     return fmt_pct(row.get("win_rate"))
        if label == "Avg Match Score":
            v = row.get("avg_match_score")
            return f"{float(v):.3f}" if pd.notna(v) else "—"
        if label == "Best Win":
            bw = row.get("best_win")
            return str(bw) if pd.notna(bw) else "—"
        return "—"

    # Header row
    h_blank, h1, h2 = st.columns([1.5, 2, 2])
    h1.markdown(
        f"### {flag(r1['nationality'])} {player1}",
    )
    h2.markdown(
        f"### {flag(r2['nationality'])} {player2}",
    )

    for label in stat_labels:
        v1 = get_stat(r1, label)
        v2 = get_stat(r2, label)

        col_label, col_v1, col_v2 = st.columns([1.5, 2, 2])
        col_label.markdown(f"**{label}**")

        if label == "Tier":
            col_v1.markdown(tier_badge(v1), unsafe_allow_html=True)
            col_v2.markdown(tier_badge(v2), unsafe_allow_html=True)
        elif label == "PAR Score":
            p1_par = float(r1["par_score"]) if pd.notna(r1["par_score"]) else 0
            p2_par = float(r2["par_score"]) if pd.notna(r2["par_score"]) else 0
            c1 = "#2ECC71" if p1_par > p2_par else ("#E74C3C" if p1_par < p2_par else "#95A5A6")
            c2 = "#2ECC71" if p2_par > p1_par else ("#E74C3C" if p2_par < p1_par else "#95A5A6")
            col_v1.markdown(f'<span style="font-weight:700;font-size:1.1rem;color:{c1};">{v1}</span>', unsafe_allow_html=True)
            col_v2.markdown(f'<span style="font-weight:700;font-size:1.1rem;color:{c2};">{v2}</span>', unsafe_allow_html=True)
        else:
            col_v1.markdown(v1)
            col_v2.markdown(v2)

        st.divider()

    # ── PAR bar chart ─────────────────────────────────────────────────────────
    st.subheader("PAR Score Comparison")

    p1_par = float(r1["par_score"]) if pd.notna(r1["par_score"]) else 0
    p2_par = float(r2["par_score"]) if pd.notna(r2["par_score"]) else 0

    fig = go.Figure(go.Bar(
        x=[player1, player2],
        y=[p1_par, p2_par],
        marker_color=[
            "#2ECC71" if p1_par >= p2_par else "#95A5A6",
            "#2ECC71" if p2_par > p1_par else "#95A5A6",
        ],
        text=[fmt_par(p1_par), fmt_par(p2_par)],
        textposition="outside",
        textfont=dict(size=14, color="#2C3E50"),
        width=0.4,
    ))

    fig.add_hline(y=0, line_dash="dash", line_color="#BDC3C7", line_width=1)

    fig.update_layout(
        height=300,
        margin=dict(l=20, r=20, t=20, b=20),
        xaxis=dict(showgrid=False),
        yaxis=dict(title="PAR Score", gridcolor="#F0F0F0", zeroline=False),
        plot_bgcolor="white",
        paper_bgcolor="white",
        showlegend=False,
    )

    st.plotly_chart(fig, use_container_width=True)

    # ── Verdict ───────────────────────────────────────────────────────────────
    st.divider()
    diff = abs(p1_par - p2_par)
    winner_name = player1 if p1_par > p2_par else player2
    loser_name  = player2 if p1_par > p2_par else player1

    if diff < 0.001:
        st.markdown("**🤝 It's a tie** — both players have equal PAR scores.")
    else:
        verdict_color = TIER_COLORS.get(
            par_scores.loc[par_scores["player_name"] == winner_name, "par_tier"].values[0],
            "#2ECC71"
        )
        st.markdown(
            f'<div style="background:#F8F9FA;border-left:4px solid {verdict_color};'
            f'padding:16px 20px;border-radius:6px;">'
            f'<span style="font-size:1.1rem;font-weight:700;">{winner_name}</span> '
            f'has the higher PAR score, leading '
            f'<span style="font-weight:700;">{loser_name}</span> '
            f'by <span style="font-size:1.2rem;font-weight:800;color:{verdict_color};">'
            f'{diff:+.3f}</span> PAR points.'
            f'</div>',
            unsafe_allow_html=True,
        )

    # ── Career arc overlay ────────────────────────────────────────────────────
    st.divider()
    st.subheader("Career Arc Overlay")

    tl1 = par_timeline[par_timeline["player_name"] == player1].sort_values("date")
    tl2 = par_timeline[par_timeline["player_name"] == player2].sort_values("date")

    if tl1.empty and tl2.empty:
        st.info("No timeline data for either player.")
    else:
        fig2 = go.Figure()
        fig2.add_hline(y=0, line_dash="dash", line_color="#BDC3C7", line_width=1)

        for tl, name, color in [(tl1, player1, "#3498DB"), (tl2, player2, "#E74C3C")]:
            if not tl.empty:
                fig2.add_trace(go.Scatter(
                    x=tl["date"],
                    y=tl["tournament_par"],
                    mode="lines+markers",
                    name=name,
                    line=dict(color=color, width=2.5),
                    marker=dict(size=7),
                    customdata=list(zip(tl["tournament_name"], tl["tier"])),
                    hovertemplate=(
                        f"<b>{name}</b><br>"
                        "%{customdata[0]}<br>"
                        "Tier: %{customdata[1]}<br>"
                        "PAR: %{y:.3f}<br>"
                        "Date: %{x|%b %Y}<extra></extra>"
                    ),
                ))

        fig2.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=20),
            xaxis=dict(title="", showgrid=False),
            yaxis=dict(title="Tournament PAR", zeroline=False, gridcolor="#F0F0F0"),
            plot_bgcolor="white",
            paper_bgcolor="white",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            hovermode="closest",
        )

        st.plotly_chart(fig2, use_container_width=True)
