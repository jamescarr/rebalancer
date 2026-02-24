"""Seed the database with example crypto strategies."""

from app.database import SessionLocal
from app.models import Strategy

SEED_STRATEGIES = [
    {
        "name": "Blue Chip HODL",
        "description": "Heavy BTC/ETH core with a small LTC position. Classic store-of-value thesis.",
        "strategy_type": "fixed_weight",
        "assets": ["BTCUSD", "ETHUSD", "LTCUSD"],
        "config": {
            "weights": {"BTCUSD": 0.55, "ETHUSD": 0.35, "LTCUSD": 0.10},
        },
        "rebalance_interval_seconds": 86400,
        "drift_threshold": 0.05,
        "is_active": False,
    },
    {
        "name": "Alt Season",
        "description": "Equal-weight basket of mid-cap alts. No BTC or ETH. Rebalances every 10 minutes.",
        "strategy_type": "equal_weight",
        "assets": ["SOLUSD", "AVAXUSD", "DOTUSD", "LINKUSD", "UNIUSD"],
        "config": {},
        "rebalance_interval_seconds": 600,
        "drift_threshold": 0.03,
        "is_active": False,
    },
    {
        "name": "Momentum Top 3",
        "description": "Rotate into the top 3 performers across a broad crypto basket (7-day lookback). Rebalances every 30 seconds for demo.",
        "strategy_type": "momentum",
        "assets": ["BTCUSD", "ETHUSD", "SOLUSD", "DOGEUSD", "SHIBUSD", "AAVEUSD"],
        "config": {
            "lookback_days": 7,
            "top_n": 3,
            "top_weight": 0.75,
        },
        "rebalance_interval_seconds": 30,
        "drift_threshold": 0.02,
        "is_active": False,
    },
    {
        "name": "Contrarian Surge",
        "description": "15 cryptos, mean-reversion + rotating surge. Overweights the biggest losers (buy the dip), plus one asset gets a 15% bonus that rotates every 30 seconds. Guaranteed to trade every cycle.",
        "strategy_type": "contrarian_surge",
        "assets": ["BTCUSD", "ETHUSD", "SOLUSD", "AVAXUSD", "DOTUSD", "LINKUSD", "UNIUSD", "AAVEUSD", "LTCUSD", "DOGEUSD", "SHIBUSD", "BCHUSD", "GRTUSD", "CRVUSD", "SUSHIUSD"],
        "config": {"surge_pct": 0.15, "contrarian_weight": 0.60, "lookback_days": 1},
        "rebalance_interval_seconds": 30,
        "drift_threshold": 0.001,
        "is_active": False,
    },
]


def seed() -> None:
    with SessionLocal() as db:
        existing = db.query(Strategy).count()
        if existing > 0:
            print(f"Database already has {existing} strategies, skipping seed.")
            return

        for data in SEED_STRATEGIES:
            db.add(Strategy(**data))
        db.commit()
        print(f"Seeded {len(SEED_STRATEGIES)} strategies.")


if __name__ == "__main__":
    seed()
