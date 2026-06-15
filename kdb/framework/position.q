\c 50 300

// ============================================================================
// position.q - Position tracking, P&L, funding, auto-exit policies (v0.1)
//
// Owns the single source of truth for "where am I right now":
//   - net position per symbol
//   - cost basis (volume-weighted entry price)
//   - realized P&L (closed trades)
//   - unrealized P&L (mark-to-market on open positions)
//   - cumulative fees paid
//   - cumulative funding paid/received
//
// Also runs auto-exit policies (stop-loss, max-drawdown kill) on each
// position update. Policies emit intents that the framework routes through
// pretrade -> execution like any other intent.
// ============================================================================


// ----------------------------------------------------------------------------
// Configuration - auto-exit policies
// ----------------------------------------------------------------------------

.pos.cfg.stopLossPct: 0.02;       / per-symbol stop-loss: 2% adverse move closes the position
.pos.cfg.killSwitchPct: 0.10;     / kill switch: if total drawdown exceeds 10%, flatten everything
.pos.cfg.initialEquity: 100000.0; / starting equity (USDT) for drawdown calc


// ----------------------------------------------------------------------------
// State - one row per symbol; multi-strategy not supported in v0.1
// ----------------------------------------------------------------------------

.pos.state.book: ([sym:`symbol$()]
  qty:           `float$();
  costBasis:     `float$();
  realizedPnl:   `float$();
  feesPaid:      `float$();
  fundingPaid:   `float$();
  lastMarkPrice: `float$();
  peakUnrlz:     `float$()
  );

/ Column meaning:
/   qty           - net position (positive = long, negative = short)
/   costBasis     - volume-weighted average cost per unit (for open position)
/   realizedPnl   - closed-trade P&L (USDT)
/   feesPaid      - cumulative fees in USDT
/   fundingPaid   - cumulative funding paid (negative = received)
/   lastMarkPrice - last seen trade price (used for MTM)
/   peakUnrlz     - peak unrealized P&L since position opened (for future trailing logic)

.pos.state.killSwitchTriggered: 0b;


// ----------------------------------------------------------------------------
// Public reads
// ----------------------------------------------------------------------------

.pos.qty: {[sym]
  r: .pos.state.book[sym];
  $[null r `qty; 0f; r `qty]
 };

.pos.unrealizedPnl: {[sym]
  r: .pos.state.book[sym];
  if[null r `qty; :0f];
  if[0f = r `qty; :0f];
  / unrealized = (markPrice - costBasis) * qty  (works for long and short)
  (r[`lastMarkPrice] - r `costBasis) * r `qty
 };

.pos.totalEquity: {[]
  realized: sum exec realizedPnl from .pos.state.book;
  fees:     sum exec feesPaid    from .pos.state.book;
  funding:  sum exec fundingPaid from .pos.state.book;
  syms:     exec sym from .pos.state.book;
  unrlz:    sum .pos.unrealizedPnl each syms;
  .pos.cfg.initialEquity + realized - fees - funding + unrlz
 };

/ Compact snapshot for pretrade gate
.pos.snapshot: {[]
  / Returns a dict: sym -> qty (only open positions). Used by pretrade.check.
  open: select sym, qty from .pos.state.book where qty <> 0f;
  (open `sym) ! (open `qty)
 };


// ----------------------------------------------------------------------------
// Fill application
//
// A fill carries: sym, fillPrice, qty (signed: positive=long-add, negative=short-add),
//                 feeUsdt, source (`strategy or `policy)
// ----------------------------------------------------------------------------

.pos.applyFill: {[fill]
  sym: fill `sym;
  fillQty: fill `qty;        / signed
  fillPx:  fill `fillPrice;
  fee:     fill `feeUsdt;

  cur: .pos.state.book[sym];
  curQty:  $[null cur `qty;          0f; cur `qty];
  curCost: $[null cur `costBasis;    0f; cur `costBasis];
  curRlz:  $[null cur `realizedPnl;  0f; cur `realizedPnl];
  curFees: $[null cur `feesPaid;     0f; cur `feesPaid];
  curFund: $[null cur `fundingPaid;  0f; cur `fundingPaid];

  newQty: curQty + fillQty;
  newRlz: curRlz;
  newCost: curCost;

  / Case analysis:
  /   - opening from flat: cost = fillPx, no realized
  /   - adding to same-side: weighted-average cost, no realized
  /   - reducing (partial close): realize on the reduced portion at curCost vs fillPx
  /   - flipping (cross through zero): realize the full close, then open opposite
  $[curQty = 0f;
    [newCost: fillPx];
   (signum curQty) = signum fillQty;
    [newCost: ((curCost * abs curQty) + (fillPx * abs fillQty)) % (abs curQty) + abs fillQty];
   (abs fillQty) <= abs curQty;
    [
      / Reducing: realize on the closed slice; cost basis unchanged
      / For a long position (curQty > 0) being reduced by selling at fillPx:
      /   pnl_per_unit = fillPx - curCost
      / For a short (curQty < 0) being reduced by buying at fillPx:
      /   pnl_per_unit = curCost - fillPx
      pnlPerUnit: $[curQty > 0f; fillPx - curCost; curCost - fillPx];
      newRlz: curRlz + (abs fillQty) * pnlPerUnit;
      newCost: curCost;     / unchanged: still holding partial position
    ];
   / Flipping through zero
    [
      closeQty: abs curQty;
      openQty:  (abs fillQty) - closeQty;
      pnlPerUnit: $[curQty > 0f; fillPx - curCost; curCost - fillPx];
      newRlz: curRlz + closeQty * pnlPerUnit;
      newCost: fillPx;      / new position opened at fillPx
    ]
   ];

  newFees: curFees + fee;
  / lastMarkPrice and peakUnrlz reset/update
  newPeak: $[newQty = 0f; 0f; 0f]; / will be updated on next markPrice

  upd: enlist `sym`qty`costBasis`realizedPnl`feesPaid`fundingPaid`lastMarkPrice`peakUnrlz !
       (sym; newQty; newCost; newRlz; newFees; curFund; fillPx; newPeak);
  `.pos.state.book upsert upd;
 };


// ----------------------------------------------------------------------------
// Mark price update (called on every trade event so MTM is current)
// ----------------------------------------------------------------------------

.pos.updateMarkPrice: {[sym; price]
  cur: .pos.state.book[sym];
  if[null cur `qty;
    / no row yet for this sym - create one with zero position so we can track price
    upd: enlist `sym`qty`costBasis`realizedPnl`feesPaid`fundingPaid`lastMarkPrice`peakUnrlz !
         (sym; 0f; 0f; 0f; 0f; 0f; price; 0f);
    `.pos.state.book upsert upd;
    :()
  ];
  / Update mark price only. peakUnrlz exists in schema for future trailing-
  / stop logic but is unused in v0.1; leaving it untouched here.
  upd: enlist `sym`lastMarkPrice ! (sym; price);
  `.pos.state.book upsert upd;
 };


// ----------------------------------------------------------------------------
// Funding application
//
// At each funding boundary, every open position is charged based on:
//   chargeUsdt = abs(qty) * markPrice * fundingRate   if long and rate > 0  : pay
//                                                     if long and rate < 0  : receive
//                                                     if short and rate > 0 : receive
//                                                     if short and rate < 0 : pay
// Sign convention in fundingPaid: positive = paid out (cost), negative = received
// ----------------------------------------------------------------------------

.pos.applyFunding: {[sym; rate; markPrice]
  cur: .pos.state.book[sym];
  if[null cur `qty; :()];
  if[0f = cur `qty; :()];

  notional: (abs cur `qty) * markPrice;
  / charge sign: long pays when rate>0; short pays when rate<0
  charge: notional * rate * signum cur `qty;
  / charge here is positive when this position must pay
  / fundingPaid is cumulative positive=paid
  upd: enlist `sym`fundingPaid ! (sym; (cur `fundingPaid) + charge);
  `.pos.state.book upsert upd;
 };


// ----------------------------------------------------------------------------
// Auto-exit policies
//
// Called after every trade event AND every timer tick. Returns a list of
// intents (possibly empty). Each intent has source=`policy so the
// pretrade/execution chain knows where it came from.
//
// v0.1 policies:
//   - stop-loss: if unrealized PnL on an open position has fallen by
//     stopLossPct of its notional, emit a flatten intent.
//   - kill switch: if total equity has dropped by killSwitchPct from
//     initial, flatten everything and refuse further intents.
// ----------------------------------------------------------------------------

.pos.checkPolicies: {[]
  intents: ();

  / Kill switch first
  curEquity: .pos.totalEquity[];
  drawdownPct: 1f - (curEquity % .pos.cfg.initialEquity);
  if[(drawdownPct >= .pos.cfg.killSwitchPct) & (not .pos.state.killSwitchTriggered);
    -1 raze ("POS: KILL SWITCH triggered (drawdown="; string drawdownPct; ")");
    .pos.state.killSwitchTriggered: 1b;
    flat: select sym from .pos.state.book where qty <> 0f;
    intents: {`action`sym`qty`source!(`flatten; x; 0f; `policy)} each flat `sym;
    :intents
  ];

  / Stop-loss per symbol
  open: select sym, qty, costBasis, lastMarkPrice from .pos.state.book where qty <> 0f;
  {[r]
    notional: (abs r `qty) * r `costBasis;
    unrlz: (r[`lastMarkPrice] - r `costBasis) * r `qty;
    if[notional > 0f;
      lossPct: neg unrlz % notional;
      if[lossPct >= .pos.cfg.stopLossPct;
        -1 raze ("POS: stop-loss "; string r `sym; " (loss="; string lossPct; ")");
        intents,: enlist `action`sym`qty`source!(`flatten; r `sym; 0f; `policy);
      ];
    ];
   } each open;

  intents
 };


// ----------------------------------------------------------------------------
// Reporting
// ----------------------------------------------------------------------------

.pos.report: {[]
  -1 "============================================";
  -1 "POSITION REPORT";
  -1 "============================================";
  / Pull the column values explicitly via exec so we sum the actual numbers.
  / `sum keyed_table \`col` did not aggregate correctly in v0.1.
  totalRealized: sum exec realizedPnl from .pos.state.book;
  totalFees:     sum exec feesPaid    from .pos.state.book;
  totalFunding:  sum exec fundingPaid from .pos.state.book;
  -1 raze ("  Initial equity:  $"; string .pos.cfg.initialEquity);
  -1 raze ("  Current equity:  $"; string .pos.totalEquity[]);
  -1 raze ("  Total realized:  $"; string totalRealized);
  -1 raze ("  Total fees:      $"; string totalFees);
  -1 raze ("  Total funding:   $"; string totalFunding);
  -1 raze ("  Net (rlz-fees-fund): $"; string totalRealized - totalFees + totalFunding);
  -1 raze ("  Kill triggered:  "; string .pos.state.killSwitchTriggered);
  -1 "";
  -1 "Per-symbol detail:";
  show .pos.state.book;
 };

-1 "position.q loaded";
