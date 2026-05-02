# Dollar Imbalance Bars (DIB) — buyerIsMaker Method

## Overview

Dollar Imbalance Bars sample when cumulative signed dollar volume becomes unusually one-sided. Unlike time bars, they capture moments of abnormal order flow imbalance.

---

## Raw Data

| Tick | Price | Qty | buyerIsMaker |
|------|-------|-----|--------------|
| 1 | 100.00 | 0.50 | 0 |
| 2 | 100.02 | 0.30 | 0 |
| 3 | 100.02 | 0.25 | 1 |
| 4 | 100.02 | 0.40 | 1 |
| 5 | 100.01 | 0.35 | 1 |
| 6 | 100.03 | 0.20 | 0 |
| 7 | 100.03 | 0.45 | 0 |
| 8 | 100.03 | 0.15 | 1 |
| 9 | 100.05 | 0.60 | 0 |
| 10 | 100.04 | 0.25 | 1 |
| 11 | 100.04 | 0.30 | 0 |
| 12 | 100.04 | 0.20 | 0 |
| 13 | 100.06 | 0.55 | 0 |
| 14 | 100.06 | 0.35 | 1 |
| 15 | 100.05 | 0.40 | 1 |
| 16 | 100.07 | 0.50 | 0 |
| 17 | 100.07 | 0.25 | 1 |
| 18 | 100.07 | 0.30 | 1 |
| 19 | 100.08 | 0.45 | 0 |
| 20 | 100.08 | 0.35 | 0 |

---

## Step 1: Sign Each Trade

**Formula:**

```
b_t = 1 - 2 × buyerIsMaker
```

**Interpretation:**

| buyerIsMaker | b_t | Meaning |
|--------------|-----|---------|
| 0 | +1 | Buyer aggressed (taker) |
| 1 | -1 | Seller aggressed (taker) |

---

## Step 2: Calculate Signed Dollar

**Formula:**

```
Signed $ = b_t × Price × Qty
```

**Result:**

| Tick | Price × Qty | b_t | Signed $ |
|------|-------------|-----|----------|
| 1 | 50.00 | +1 | +50.00 |
| 2 | 30.01 | +1 | +30.01 |
| 3 | 25.01 | -1 | -25.01 |
| 4 | 40.01 | -1 | -40.01 |
| 5 | 35.00 | -1 | -35.00 |
| 6 | 20.01 | +1 | +20.01 |
| 7 | 45.01 | +1 | +45.01 |
| 8 | 15.00 | -1 | -15.00 |
| 9 | 60.03 | +1 | +60.03 |
| 10 | 25.01 | -1 | -25.01 |
| 11 | 30.01 | +1 | +30.01 |
| 12 | 20.01 | +1 | +20.01 |
| 13 | 55.03 | +1 | +55.03 |
| 14 | 35.02 | -1 | -35.02 |
| 15 | 40.02 | -1 | -40.02 |
| 16 | 50.04 | +1 | +50.04 |
| 17 | 25.02 | -1 | -25.02 |
| 18 | 30.02 | -1 | -30.02 |
| 19 | 45.04 | +1 | +45.04 |
| 20 | 35.03 | +1 | +35.03 |

---

## Step 3: Calculate Threshold (AFML Formula)

**Parameters:**

| Parameter | Description | Formula | Value |
|-----------|-------------|---------|-------|
| E[T] | Expected ticks per bar | Target (e.g., want ~2 bars from 20 ticks) | 10 |
| P[b=1] | Probability tick is a buy | Count(buyerIsMaker=0) / Total ticks = 11/20 | 0.55 |
| E[d] | Expected dollar per tick | Avg(Price × Qty) | 35.52 |

**AFML Threshold Formulas:**

For Dollar Imbalance Bars (DIB):
```
Threshold = E[T] × E[d] × |2P[b=1] - 1|
          = 10 × 35.52 × |2(0.55) - 1|
          = 10 × 35.52 × 0.10
          = 35.52
```

For Dollar Run Bars (DRB):
```
Threshold = E[T] × E[d] × max(P[b=1], 1 - P[b=1])
          = 10 × 35.52 × max(0.55, 0.45)
          = 10 × 35.52 × 0.55
          = 195.36
```

**Interpretation:**

The threshold represents expected imbalance under normal conditions. When actual imbalance exceeds this, something unusual is happening.

---

## Step 4: Accumulate θ and Sample

**Rules for DIB:**

1. Accumulate signed dollars into θ (cumulative imbalance)
2. When |θ| ≥ Threshold → Bar forms
3. Reset θ to 0 and start new bar

**Rules for DRB:**

1. Accumulate signed dollars into θ
2. **Reset θ when trade direction changes** (unlike DIB where signs cancel)
3. When |θ| ≥ Threshold → Bar forms
4. Reset θ to 0 and start new bar

**DIB Result (Threshold = 35.52):**

| Tick | Signed $ | θ | |θ| | Bar? |
|------|----------|---|-----|------|
| 1 | +50.00 | +50.00 | 50.00 | YES |
| 2 | +30.01 | +30.01 | 30.01 | NO |
| 3 | -25.01 | +5.00 | 5.00 | NO |
| 4 | -40.01 | -35.01 | 35.01 | NO |
| 5 | -35.00 | -70.01 | 70.01 | YES |
| 6 | +20.01 | +20.01 | 20.01 | NO |
| 7 | +45.01 | +65.02 | 65.02 | YES |
| 8 | -15.00 | -15.00 | 15.00 | NO |
| 9 | +60.03 | +45.03 | 45.03 | YES |
| 10 | -25.01 | -25.01 | 25.01 | NO |
| 11 | +30.01 | +5.00 | 5.00 | NO |
| 12 | +20.01 | +25.01 | 25.01 | NO |
| 13 | +55.03 | +80.04 | 80.04 | YES |
| 14 | -35.02 | -35.02 | 35.02 | NO |
| 15 | -40.02 | -75.04 | 75.04 | YES |
| 16 | +50.04 | +50.04 | 50.04 | YES |
| 17 | -25.02 | -25.02 | 25.02 | NO |
| 18 | -30.02 | -55.04 | 55.04 | YES |
| 19 | +45.04 | +45.04 | 45.04 | YES |
| 20 | +35.03 | +35.03 | 35.03 | NO |

*Note: θ resets to 0 after each bar forms.*

---

## Result: 9 Bars

| Bar | Ticks | O | H | L | C | θ_final |
|-----|-------|-------|-------|-------|-------|---------|
| 1 | 1 | 100.00 | 100.00 | 100.00 | 100.00 | +50 |
| 2 | 2-5 | 100.02 | 100.02 | 100.01 | 100.01 | -70 |
| 3 | 6-7 | 100.03 | 100.03 | 100.03 | 100.03 | +65 |
| 4 | 8-9 | 100.03 | 100.05 | 100.03 | 100.05 | +45 |
| 5 | 10-13 | 100.04 | 100.06 | 100.04 | 100.06 | +80 |
| 6 | 14-15 | 100.06 | 100.06 | 100.05 | 100.05 | -75 |
| 7 | 16 | 100.07 | 100.07 | 100.07 | 100.07 | +50 |
| 8 | 17-18 | 100.07 | 100.07 | 100.07 | 100.07 | -55 |
| 9 | 19 | 100.08 | 100.08 | 100.08 | 100.08 | +45 |

---

## What θ_final Means

| θ_final | Meaning |
|---------|---------|
| > 0 | Bar closed with net buy pressure |
| < 0 | Bar closed with net sell pressure |

This is descriptive, not prescriptive. It tells you what happened, not what to do.

---

## Step 5: Adaptive Threshold (AFML EWMA)

After each bar closes, update parameters using exponential weighted moving average:

```
α = 2 / (span + 1)

E[T]_new    = α × T_actual + (1-α) × E[T]_old
P[b=1]_new  = α × (buys_in_bar / T_actual) + (1-α) × P[b=1]_old
E[d]_new    = α × (dollar_in_bar / T_actual) + (1-α) × E[d]_old

Threshold_new = E[T]_new × E[d]_new × |2P[b=1]_new - 1|   (DIB)
Threshold_new = E[T]_new × E[d]_new × max(P, 1-P)         (DRB)
```

This allows the threshold to adapt to changing market conditions.

---

## AFML Limitations on High-Frequency Crypto Data

Empirical testing on Binance BTCUSDT tick data (5M+ ticks/day) revealed that the pure AFML formula fails catastrophically:

### Problem 1: E[d] Explosion

Trade sizes follow a **fat-tailed distribution**:

| Trade Type | Dollar Value |
|------------|--------------|
| Typical (85%) | < $50 |
| Medium | $50 - $1,000 |
| Whale | $10,000 - $100,000+ |

When a bar captures whale trades:
```
Bar E[d] = $10,000,000 / 1,000 ticks = $10,000/tick
Global E[d] = $400/tick

EWMA update: new_E[d] = 0.02 × 10,000 + 0.98 × 400 = $592
```

This 48% increase from **one bar** compounds rapidly:

| Day | E[d] | Threshold |
|-----|------|-----------|
| 1 (warmup) | $411 | $1.3M |
| 2 | $17,622 | $29M |
| 3 | $16,946 | $174M |

Result: **Threshold explodes 138× in 3 days**, no bars form.

### Problem 2: |2P - 1| Instability

For crypto orderflow, P[b=1] ≈ 0.5 (balanced market):

| P[b=1] | |2P - 1| | Impact |
|--------|---------|--------|
| 0.50 | 0.00 | Threshold → 0 (too many bars) |
| 0.51 | 0.02 | Very small multiplier |
| 0.55 | 0.10 | Still small |

Small fluctuations in P cause large threshold swings:
- Jan 14: P=0.557, |2P-1|=0.114
- Jan 22: P=0.494, |2P-1|=0.012 (9× smaller!)

### Problem 3: DRB Threshold Too High

For DRB with P ≈ 0.5:
```
DIB: |2×0.51 - 1| = 0.02
DRB: max(0.51, 0.49) = 0.51
```

DRB threshold is **25× higher** than DIB for balanced orderflow — almost no bars form.

### Empirical Results

| Mode | Target Bars/Day | Actual (Day 1) | Actual (Day 12) |
|------|-----------------|----------------|-----------------|
| DIB | 200 | 4,639 | 0-1 |
| DRB | 200 | 4 | 0 |

Both modes fail completely.

---

## Alternative: Random Walk Approach

Since AFML fails on this data, an alternative formulation treats cumulative signed dollar as a **random walk**:

### Theory

If signed dollars are approximately uncorrelated:
```
Var(Σ signed_dollar) ≈ n × Var(signed_dollar)
Std(Σ signed_dollar) ≈ √n × σ
```

Threshold should scale with **√E[T]**, not E[T].

### Modified Formula

```
Threshold = σ × √E[T]
```

Where σ = std(signed_dollar) from warmup day.

### Enhancements

**1. Target Anchoring (Mean-Reversion)**

Prevents drift by pulling threshold toward calibrated anchor:
```
raw_threshold = current_threshold × √(new_E[T] / old_E[T])
new_threshold = raw_threshold + κ × (anchor - raw_threshold)
```

With κ = 0.02 (2% mean-reversion per bar).

**2. Price Adjustment**

As BTC price changes, dollar thresholds should scale:
```
price_adjusted_anchor = anchor × (today_avg_price / warmup_avg_price)
```

### Comparison

| Component | AFML | Random Walk |
|-----------|------|-------------|
| E[T] scaling | Linear | **√E[T]** |
| E[d] term | Yes (explodes) | **Removed** |
| P[b=1] term | Yes (unstable) | **Removed** |
| Anchoring | No | **Yes (κ=2%)** |
| Price adjustment | No | **Yes** |

### Results

| Metric | AFML | Random Walk |
|--------|------|-------------|
| Threshold drift | 13,765% | < 50% |
| Bars per day | 0-4,639 | ~200 (stable) |
| Usable | No | Yes |

---

## Summary

### Basic DIB Construction

1. **Sign trades** using buyerIsMaker: `b_t = 1 - 2 × buyerIsMaker`
2. **Calculate signed dollar**: `Signed $ = b_t × Price × Qty`
3. **Set threshold**: Choose formula based on data characteristics
4. **Accumulate θ**, form bar when `|θ| ≥ Threshold`, then reset

### Formula Selection

| Data Type | Recommended Formula |
|-----------|---------------------|
| Low-frequency, stable | AFML: `E[T] × E[d] × |2P-1|` |
| High-frequency crypto | Random Walk: `σ × √E[T]` with anchoring |

### Key Insight

DIB gives you **when to look** — moments of unusual order flow imbalance. The threshold formula determines bar frequency; choose one that remains stable for your data.
