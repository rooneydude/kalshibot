#!/usr/bin/env python3
"""
Simple CLI dashboard -- shows current state at a glance.

Usage:
    python -m scripts.dashboard
"""

import sys
import os
import json
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import db

logging.basicConfig(level=logging.WARNING)


def main():
    db.init_db()

    print("=" * 60)
    print("  KALSHI MISPRICING BOT -- DASHBOARD")
    print("=" * 60)

    with db.get_conn() as conn:
        with db.get_cursor(conn) as cur:
            # Portfolio
            state = db.get_portfolio_state(cur)
            if state:
                state = dict(state)
                print(f"\n  Balance:        ${state.get('balance', 0):.2f}")
                print(f"  Daily P&L:      ${state.get('daily_pnl', 0):+.2f}")
                print(f"  Open Positions: {state.get('open_positions', 0)}")
                print(f"  Kill Switch:    {'ON' if state.get('kill_switch') else 'OFF'}")
            else:
                print("\n  No portfolio state yet.")

            # Markets
            cur.execute("SELECT COUNT(*) AS cnt FROM markets WHERE status = 'open'")
            row = cur.fetchone()
            print(f"\n  Open Markets:   {row['cnt'] if isinstance(row, dict) else row[0]}")

            # Relationships
            cur.execute("SELECT COUNT(*) AS cnt FROM relationships")
            row = cur.fetchone()
            print(f"  Relationships:  {row['cnt'] if isinstance(row, dict) else row[0]}")

            cur.execute("SELECT type, COUNT(*) AS cnt FROM relationships GROUP BY type ORDER BY cnt DESC")
            for r in cur.fetchall():
                t = r['type'] if isinstance(r, dict) else r[0]
                c = r['cnt'] if isinstance(r, dict) else r[1]
                print(f"    {t}: {c}")

            # Recent opportunities
            cur.execute(
                "SELECT * FROM opportunities ORDER BY detected_at DESC LIMIT 10"
            )
            opps = cur.fetchall()
            print(f"\n  Recent Opportunities ({len(opps)}):")
            for opp in opps:
                opp = dict(opp)
                print(
                    f"    [{opp.get('status', '?'):10s}] {opp.get('signal', '?'):30s} "
                    f"mag=${opp.get('magnitude', 0):.4f}  score={opp.get('score', 0):.6f}  "
                    f"@ {opp.get('detected_at', '?')[:19]}"
                )

            # Recent trades
            cur.execute(
                "SELECT * FROM trades ORDER BY created_at DESC LIMIT 10"
            )
            trades = cur.fetchall()
            print(f"\n  Recent Trades ({len(trades)}):")
            for t in trades:
                t = dict(t)
                print(
                    f"    [{t.get('order_status', '?'):10s}] {t.get('action', '?'):4s} "
                    f"{t.get('count', 0):3d}x {t.get('ticker', '?'):20s} "
                    f"@ ${t.get('price', 0):.2f}  fee=${t.get('fees', 0):.2f}  "
                    f"@ {t.get('created_at', '?')[:19]}"
                )

    print("\n" + "=" * 60)


if __name__ == "__main__":
    main()
