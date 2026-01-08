def render_prop_cards(
    df,
    *,
    require_ev_plus: bool,
    odds_min: float,
    odds_max: float,
    min_hit_rate: float,
    hit_rate_col: str = "hit_rate_last10",
    hit_label: str = "L10 Hit",
    min_opp_rank: int | None = None,
    page_key: str = "ev",
):
    """
    Shared card-grid renderer for both EV+ Props and Available Props.
    Cards are always visible; tapping the card's invisible overlay expands
    an analytics / Save Bet section underneath.
    """

    if df.empty:
        st.info("No props match your filters.")
        return

    # ------------------------------------------------------
    # WOWY merge once per render
    # ------------------------------------------------------
    card_df = attach_wowy_deltas(df, wowy_df)

    wowy_cols = [
        "breakdown",
        "pts_delta",
        "reb_delta",
        "ast_delta",
        "pra_delta",
        "pts_reb_delta",
    ]

    def extract_wowy_list(g: pd.DataFrame) -> list[dict]:
        df2 = g.copy()
        df2 = df2[wowy_cols]
        if "breakdown" in df2.columns:
            df2 = df2[df2["breakdown"].notna()]
        return df2.to_dict("records")

    w_map: dict[tuple[str, str], list[dict]] = {}
    for (player, team), g in card_df.groupby(["player", "player_team"]):
        w_map[(player, team)] = extract_wowy_list(g)

    card_df["_wowy_list"] = card_df.apply(
        lambda r: w_map.get((r["player"], r["player_team"]), []),
        axis=1,
    )

    # ------------------------------------------------------
    # Row filter (odds / hit-rate / EV+ / opponent rank)
    # ------------------------------------------------------
    def card_good(row: pd.Series) -> bool:
        price = row.get("price")
        hit = row.get(hit_rate_col)

        if pd.isna(price) or pd.isna(hit):
            return False

        if not (odds_min <= price <= odds_max):
            return False

        if hit < min_hit_rate:
            return False

        if min_opp_rank is not None:
            r = get_opponent_rank(row)
            if r is None or r < min_opp_rank:
                return False

        if require_ev_plus:
            implied = compute_implied_prob(price)
            if implied is None or hit <= implied:
                return False

        return True

    card_df = card_df[card_df.apply(card_good, axis=1)]

    if card_df.empty:
        st.info("No props match your filters (after EV/odds/hit-rate logic).")
        return

    # ------------------------------------------------------
    # Sorting: best hit-rate → best odds
    # ------------------------------------------------------
    card_df = card_df.sort_values(
        by=[hit_rate_col, "price"],
        ascending=[False, True],
    ).reset_index(drop=True)

    # ------------------------------------------------------
    # Pagination
    # ------------------------------------------------------
    page_size = 30
    total_cards = len(card_df)
    total_pages = max(1, (total_cards + page_size - 1) // page_size)

    st.write(f"Showing {total_cards} props • {total_pages} pages")

    page = st.number_input(
        "Page",
        min_value=1,
        max_value=total_pages,
        value=1,
        step=1,
        key=f"{page_key}_card_page_number",
    )

    start = (page - 1) * page_size
    end = start + page_size
    page_df = card_df.iloc[start:end]

    # Scroll wrapper
    st.markdown(
        '<div style="max-height:1100px; overflow-y:auto; padding-right:12px;">',
        unsafe_allow_html=True,
    )

    cols = st.columns(4)

    # ------------------------------------------------------
    # Helper: stat-aware averages with safe fallback
    # ------------------------------------------------------
    def get_stat_avg(row, stat_prefix, window):
        """
        window = 5 | 10 | 20
        """
        if stat_prefix:
            val = row.get(f"{stat_prefix}_last{window}")
            if val is not None:
                return val

        # Fallbacks (generic, known-good)
        if window == 10:
            return get_l10_avg(row)

        return None

    # ============================================================
    #                          CARD LOOP
    # ============================================================
    for idx, row in page_df.iterrows():
        col = cols[idx % 4]
        with col:
            # -------------------------------
            # Basic fields
            # -------------------------------
            player = row.get("player", "") or ""

            def _norm(s: str) -> str:
                return (
                    str(s)
                    .lower()
                    .replace("'", "")
                    .replace(".", "")
                    .replace("-", "")
                    .strip()
                )

            inj_status = INJURY_LOOKUP_BY_NAME.get(_norm(player))
            badge_html = ""

            if inj_status:
                s = inj_status.lower()
                if "out" in s:
                    badge_color = "#ef4444"
                elif "question" in s or "doubt" in s:
                    badge_color = "#eab308"
                else:
                    badge_color = "#3b82f6"

                badge_html = f"""
                    <span style="
                        background:{badge_color};
                        color:white;
                        padding:2px 6px;
                        font-size:0.65rem;
                        font-weight:700;
                        border-radius:6px;
                        margin-left:6px;
                        white-space:nowrap;
                    ">
                    {inj_status.upper()}
                    </span>
                """

            pretty_market = MARKET_DISPLAY_MAP.get(
                row.get("market", ""), row.get("market", "")
            )
            bet_type = str(row.get("bet_type", "")).upper()
            line = row.get("line", "")

            # Odds / hit info
            price_val = row.get("price", 0)
            try:
                odds = int(price_val)
            except (TypeError, ValueError):
                odds = 0

            implied_prob = compute_implied_prob(odds) or 0.0
            hit_val = row.get(hit_rate_col, 0.0) or 0.0

            l10_avg = get_l10_avg(row)
            l10_avg_display = f"{l10_avg:.1f}" if l10_avg is not None else "-"

            # Opponent rank
            opp_rank = get_opponent_rank(row)
            if isinstance(opp_rank, int):
                rank_display = opp_rank
                rank_color = rank_to_color(opp_rank)
            else:
                rank_display = "-"
                rank_color = "#9ca3af"

            # Sparkline (values + dates)
            spark_vals, spark_dates = get_spark_series(row)
            line_value = float(row.get("line", 0) or 0)
            spark_html = build_sparkline_bars_hitmiss(
                spark_vals,
                spark_dates,
                line_value
            )


            # Logos
            player_team = normalize_team_code(row.get("player_team", ""))
            opp_team = normalize_team_code(row.get("opponent_team", ""))

            home_logo = TEAM_LOGOS_BASE64.get(player_team, "")
            opp_logo = TEAM_LOGOS_BASE64.get(opp_team, "")

            # Sportsbook
            book = normalize_bookmaker(row.get("bookmaker", ""))
            book_logo_b64 = SPORTSBOOK_LOGOS_BASE64.get(book)

            if book_logo_b64:
                book_html = (
                    f'<img src="{book_logo_b64}" '
                    'style="height:26px; width:auto; max-width:80px; '
                    'object-fit:contain; filter:drop-shadow(0 0 6px rgba(0,0,0,0.4));" />'
                )
            else:
                book_html = (
                    '<div style="padding:3px 10px; border-radius:8px;'
                    'background:rgba(255,255,255,0.08);'
                    'border:1px solid rgba(255,255,255,0.15);'
                    'font-size:0.7rem;">'
                    f"{book}"
                    "</div>"
                )

            # Tags / WOWY block
            tags_html = build_tags_html(build_prop_tags(row))
            wowy_html = build_wowy_block(row)

            # ------------------------------------------------------
            # Card HTML
            # ------------------------------------------------------
            card_lines = [
                '<div class="prop-card">',

                # Top bar
                '<div style="display:flex; justify-content:space-between; '
                'align-items:center; margin-bottom:10px;">',

                # Left: logos
                '<div style="display:flex; align-items:center; gap:6px; min-width:70px;">'
                f'<img src="{home_logo}" style="height:20px;border-radius:4px;" />'
                '<span style="font-size:0.7rem;color:#9ca3af;">vs</span>'
                f'<img src="{opp_logo}" style="height:20px;border-radius:4px;" />'
                "</div>",

                # Center: player + market + injury
                '<div style="text-align:center; flex:1; display:flex; '
                'flex-direction:column; align-items:center;">'
                f'<div style="font-size:1.05rem;font-weight:700; display:flex; '
                f'align-items:center;">{player}{badge_html}</div>'
                f'<div style="font-size:0.82rem;color:#9ca3af;">'
                f"{pretty_market} • {bet_type} {line}</div>"
                "</div>",

                # Right: book
                '<div style="display:flex; justify-content:flex-end; min-width:70px;">'
                f"{book_html}"
                "</div>",
                "</div>",  # end top bar

                # Sparkline
                f'<div style="display:flex; justify-content:center; margin:8px 0;">'
                f"{spark_html}</div>",

                # Tags
                f'<div style="display:flex; justify-content:center; margin-bottom:6px;">'
                f"{tags_html}</div>",

                # Bottom metrics
                '<div class="prop-meta" style="margin-top:2px;">',

                "<div>"
                f'<div style="color:#e5e7eb;font-size:0.8rem;">{odds:+d}</div>'
                f'<div style="font-size:0.7rem;">Imp: {implied_prob:.0%}</div>'
                "</div>",

                "<div>"
                f'<div style="color:#e5e7eb;font-size:0.8rem;">'
                f"{hit_label}: {hit_val:.0%}</div>"
                f'<div style="font-size:0.7rem;">L10 Avg: {l10_avg_display}</div>'
                "</div>",

                "<div>"
                f'<div style="color:{rank_color};font-size:0.8rem;'
                f'font-weight:700;">{rank_display}</div>'
                '<div style="font-size:0.7rem;">Opp Rank</div>'
                "</div>",

                "</div>",  # end prop-meta

                wowy_html,
                "</div>",  # end prop-card
            ]

            card_html = "\n".join(card_lines)

            # ------------------------------------------------------
            # Stable keys
            # ------------------------------------------------------
            key_base = (
                f"{page_key}_"
                f"{row.get('player')}_"
                f"{row.get('market')}_"
                f"{row.get('line')}_"
                f"{row.get('game_id', '')}"
            )
            
            expand_key = f"{key_base}_expand"
            
            if expand_key not in st.session_state:
                st.session_state[expand_key] = False
            
            
            # ======================================================
            # DERIVED METRICS (BUILT FIRST — IMPORTANT)
            # ======================================================
            display_market = row.get("market")
            raw_stat = row.get("stat_type")
            
            STAT_PREFIX_MAP = {
                "PTS": "pts",
                "REB": "reb",
                "AST": "ast",
                "STL": "stl",
                "BLK": "blk",
                "PRA": "pra",
                "PA":  "pa",
                "PR":  "pr",
                "RA":  "ra",
                "POINTS": "pts",
                "REBOUNDS": "reb",
                "ASSISTS": "ast",
                "STEALS": "stl",
                "BLOCKS": "blk",
            }
            
            stat_prefix = STAT_PREFIX_MAP.get(
                str(raw_stat).strip().upper()
                if raw_stat is not None
                else None
            )
            
            try:
                line_val = float(row.get("line"))
            except Exception:
                line_val = None
            
            l5_avg  = get_stat_avg(row, stat_prefix, 5)
            l10_avg = get_stat_avg(row, stat_prefix, 10)
            l20_avg = get_stat_avg(row, stat_prefix, 20)
            
            delta_vs_line = (
                (l10_avg - line_val)
                if l10_avg is not None and line_val is not None
                else None
            )
            
            confidence_score, confidence_level = compute_confidence(
                hit_rate_l10=row.get("hit_rate_last10"),
                delta_vs_line=delta_vs_line,
                opp_rank=row.get("opp_rank"),
            )
            
            CONF_COLORS = {
                "Strong": "#22c55e",
                "Medium": "#eab308",
                "Light":  "#60a5fa",
            }
            conf_color = CONF_COLORS.get(confidence_level, "#9ca3af")
            
            est_minutes = row.get("est_minutes")
            l5_min_avg = row.get("l5_min_avg")
            
            minutes_delta = (
                est_minutes - l5_min_avg
                if est_minutes is not None and l5_min_avg is not None
                else None
            )
            
            
            # ======================================================
            # WOWY / INJURY LINES (BUILT FIRST)
            # ======================================================
            injury_lines = []
            
            stat_type = row.get("stat_type")
            wowy_col = WOWY_MARKET_MAP.get(stat_type)
            wowy_breakdown = row.get("breakdown")
            
            if wowy_col and isinstance(wowy_breakdown, str) and wowy_breakdown.strip():
                blocks = [b.strip() for b in wowy_breakdown.split(";") if b.strip()]
            
                for block in blocks:
                    if "→" not in block:
                        continue
            
                    name_part, stats_part = block.split("→", 1)
                    stats = [s.strip() for s in stats_part.split(",") if s.strip()]
                    matched = [s for s in stats if s.startswith(f"{stat_type}=")]
            
                    if not matched:
                        continue
            
                    injury_lines.extend([
                        f"<div style='margin-top:6px; font-weight:800; font-size:0.8rem;'>"
                        f"{name_part.strip()} (Out)</div>",
                        f"<div style='font-size:0.74rem; padding-left:8px; color:#cbd5e1;'>"
                        f"{matched[0]}</div>",
                    ])
            
            
            # ======================================================
            # BUILD EXPANDED HTML (ALWAYS BUILT)
            # ======================================================
            expanded_lines = [
                f"<div style='padding:12px; margin-top:8px; border-radius:12px;"
                f"background:rgba(255,255,255,0.05);"
                f"border:1px solid rgba(255,255,255,0.12);'>",
            
                f"<div style='display:flex; justify-content:space-between; "
                f"font-size:0.78rem; margin-bottom:6px;'>",
                f"<div>L5: {_fmt1(l5_avg)}</div>",
                f"<div>L10: {_fmt1(l10_avg)}</div>",
                f"<div>L20: {_fmt1(l20_avg)}</div>",
                f"</div>",
            
                f"<div style='display:flex; justify-content:space-between; "
                f"font-size:0.8rem; margin-bottom:6px;'>",
                f"<div>Δ Line: {_fmt_signed1(delta_vs_line)}</div>",
                f"<div style='font-weight:800; color:{conf_color};'>"
                f"Confidence: {confidence_level} ({confidence_score})</div>",
                f"</div>",
            
                f"<div style='display:flex; justify-content:space-between; "
                f"font-size:0.78rem; margin-bottom:8px;'>",
                f"<div>Proj Min: {_fmt1(est_minutes)}</div>",
                f"<div>Δ Min (L5): {_fmt_signed1(minutes_delta)}</div>",
                f"</div>",
            
                f"<div style='font-size:0.82rem; font-weight:800; margin-bottom:4px;'>"
                f"Injured Teammates (WOWY Impact)</div>",
            ]
            
            if injury_lines:
                expanded_lines.extend(injury_lines)
            else:
                expanded_lines.append(
                    f"<div style='font-size:0.75rem; color:#9ca3af;'>"
                    f"No impactful teammate injuries</div>"
                )
            
            # ------------------------
            # Save Bet (bottom-right)
            # ------------------------
            expanded_lines.extend([
                f"<div style='display:flex; justify-content:flex-end; margin-top:10px;'>",
                f"<button "
                f"style='background:rgba(34,197,94,0.12); "
                f"border:1px solid rgba(34,197,94,0.35); "
                f"color:#22c55e; "
                f"font-size:0.78rem; "
                f"font-weight:700; "
                f"padding:6px 12px; "
                f"border-radius:999px; "
                f"cursor:pointer;'>"
                f"💾 Save Bet</button>",
                f"</div>",
            ])
            
            expanded_lines.append("</div>")
            expanded_html = "\n".join(expanded_lines)
            
            
            # ======================================================
            # CARD + ATTACHED EXPAND BUTTON
            # ======================================================
            with st.container():
            
                # Render card
                st.markdown(card_html, unsafe_allow_html=True)
            
                # Expand / collapse button
                expand_label = (
                    "Collapse ▴"
                    if st.session_state.get(expand_key, False)
                    else "Click to expand ▾"
                )
            
                st.button(
                    expand_label,
                    key=f"{expand_key}_btn",
                    on_click=toggle_expander,
                    args=(expand_key,),
                    use_container_width=True,
                )
            
            
            # ======================================================
            # EXPANDED SECTION (RENDER ONLY WHEN OPEN)
            # ======================================================
            if st.session_state.get(expand_key, False):
                st.markdown(expanded_html, unsafe_allow_html=True)
            

    # Close scroll wrapper
    st.markdown("</div>", unsafe_allow_html=True)

