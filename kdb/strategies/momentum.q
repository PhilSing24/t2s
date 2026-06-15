\c 50 300

// ============================================================================
// strategies/momentum.q - Example strategy: EMA-crossover long-flat (v0.1)
//
// Pure alpha logic. No stop-loss, no kill switch, no position sizing rules
// in here - all of that lives in the framework (pretrade.q / position.q).
//
// LOGIC:
//   - Compute fast EMA and slow EMA of trade prices
//   - When fast > slow and we are flat: emit buy intent for cfg.tradeQty
//   - When fast < slow and we are long: emit flatten intent
//   - We never go short in this v0.1 example
//
// HONEST CAVEAT:
//   This is a near-canonical bad strategy. EMA crossover momentum on a single
//   symbol's trade stream is what every beginner tries first and almost
//   universally loses money on after fees. It exists here to exercise the
//   framework end-to-end with a complete, readable strategy - NOT because it
//   represents alpha.
// ============================================================================

.strat.momentum.cfg.fastN: 100;     / fast EMA period (in trade events)
.strat.momentum.cfg.slowN: 500;     / slow EMA period
.strat.momentum.cfg.tradeQty: 0.1;  / BTC per trade

// ----------------------------------------------------------------------------
// init - returns initial state dict
// ----------------------------------------------------------------------------
.strat.momentum.init: {[cfg]
  / cfg may override defaults
  fast: $[`fastN in key cfg; cfg `fastN; .strat.momentum.cfg.fastN];
  slow: $[`slowN in key cfg; cfg `slowN; .strat.momentum.cfg.slowN];
  qty:  $[`tradeQty in key cfg; cfg `tradeQty; .strat.momentum.cfg.tradeQty];
  / EMA alpha = 2/(N+1). Pre-compute once.
  `fastAlpha`slowAlpha`fastEma`slowEma`primed`tradesSeen`tradeQty !
  (2f%1+fast;  2f%1+slow;  0f;  0f;  0b;  0;  qty)
 };

// ----------------------------------------------------------------------------
// onTrade - called for each trade event
//   trade is a dict with: sym, price, qty, exchTradeTs, ...
//   state is what we returned from init (or previous onTrade)
//   returns (newState; intents)
// ----------------------------------------------------------------------------
.strat.momentum.onTrade: {[state; trade]
  px: trade `price;
  sym: trade `sym;

  / Update EMAs. If this is the first tick ever, initialise both EMAs to px
  / so they don't start crossed at the wrong direction.
  $[state `tradesSeen;
    [
      newFast: (state[`fastAlpha] * px) + (1f - state `fastAlpha) * state `fastEma;
      newSlow: (state[`slowAlpha] * px) + (1f - state `slowAlpha) * state `slowEma;
    ];
    [
      newFast: px;
      newSlow: px;
    ]
  ];

  newSeen: 1 + state `tradesSeen;
  / Consider EMAs "primed" once we've seen at least 2*slowN trades
  newPrimed: newSeen > 2 * (`long$2f%state[`slowAlpha]) - 1;

  newState: state;
  newState[`fastEma]:    newFast;
  newState[`slowEma]:    newSlow;
  newState[`tradesSeen]: newSeen;
  newState[`primed]:     newPrimed;

  intents: ();

  if[newPrimed;
    curPos: .pos.qty sym;
    longSignal:  newFast > newSlow;
    flatSignal:  newFast < newSlow;
    / Enter long if signal positive and currently flat
    if[longSignal & (curPos = 0f);
      intents: enlist `action`sym`qty`source ! (`buy; sym; state `tradeQty; `strategy);
    ];
    / Exit if signal flips negative and currently long
    if[flatSignal & (curPos > 0f);
      intents: enlist `action`sym`qty`source ! (`flatten; sym; 0f; `strategy);
    ];
  ];

  (newState; intents)
 };

// ----------------------------------------------------------------------------
// onFill - optional. Lets the strategy know a fill happened. We don't need
// this for the EMA strategy (position state lives in the framework, queried
// via .pos.qty) but include a no-op so the contract is visible.
// ----------------------------------------------------------------------------
.strat.momentum.onFill: {[state; fill]
  state
 };

-1 "strategies/momentum.q loaded";
