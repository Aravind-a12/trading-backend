def calculate_depth_bands(bids, asks, best_bid, best_ask, levels=(1, 2.5, 5, 10, 25)):
    bands = {}
    ranges = [(0, levels[0])] + [(levels[i], levels[i + 1]) for i in range(len(levels) - 1)]

    for r in ranges:
        bands[f"bid_{r[0]}_{r[1]}"] = 0
        bands[f"ask_{r[0]}_{r[1]}"] = 0

    for price, qty in bids.items():
        pct_diff = (best_bid - price) / best_bid * 100
        for r in ranges:
            if r[0] <= pct_diff < r[1]:
                bands[f"bid_{r[0]}_{r[1]}"] += qty
                break

    for price, qty in asks.items():
        pct_diff = (price - best_ask) / best_ask * 100
        for r in ranges:
            if r[0] <= pct_diff < r[1]:
                bands[f"ask_{r[0]}_{r[1]}"] += qty
                break

    return bands
