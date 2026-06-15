\c 50 300

// ============================================================================
// execution.q - Paper-fill simulator with synthetic spread + Binance fees (v0.1)
//
// Takes approved intents from the pretrade layer, returns fill records to
// the framework. Models:
//
//   - Synthetic bid/ask reconstructed from last trade price plus a configured
//     half-spread (in bps). Market buys fill at synthetic_ask, market sells
//     fill at synthetic_bid.
//
//   - Binance USDS-M futures Regular tier fees: 0.05% taker, 0.02% maker.
//     v0.1 supports market orders only, so all fills are taker. 10% BNB
//     discount is configurable.
//
//   - No size impact (any size fills at the same synthetic price). True for
//     small orders; an active lie for large orders. v0.1 acceptable for
//     prototype-sizing.
//
//   - No partial fills, no limit-order modelling, no time-to-fill delay.
//
// Intent shape (input):
//   `action`sym`qty`source ! (`buy|`sell|`flatten; `BTCUSDT; 0.1f; `strategy)
//
// Fill shape (output - returned by .exec.fill):
//   `sym`fillPrice`qty`feeUsdt`source !
//   (`BTCUSDT; 60500.0; 0.1f; 3.025; `strategy)
//   qty is signed: positive=long-add, negative=short-add (sell qty -> negative)
//
// Returns null (::) if execution can't proceed (no price seen yet for that
// symbol). The framework treats this as a no-fill, intent dropped.
// ============================================================================


// ----------------------------------------------------------------------------
// Configuration
// ----------------------------------------------------------------------------

.exec.cfg.halfSpreadBps:    0.5;     / synthetic half-spread (bps); 0.5 => 1bp full spread
.exec.cfg.takerFeeBps:      5.0;     / Binance USDS-M Regular taker fee (0.05% = 5 bps)
.exec.cfg.makerFeeBps:      2.0;     / Binance USDS-M Regular maker fee (0.02% = 2 bps)
.exec.cfg.bnbDiscountPct:   0;       / 10 if using BNB to pay fees; 0 otherwise


// ----------------------------------------------------------------------------
// Last-price tracker (one entry per symbol)
// ----------------------------------------------------------------------------

.exec.state.lastPrice: (`symbol$()) ! `float$();

.exec.recordTrade: {[trade]
  / Called by framework on every market trade event so synthetic bid/ask is
  / current. Also forwards to position tracker for mark-to-market.
  sym: trade `sym;
  px:  trade `price;
  .exec.state.lastPrice[sym]: px;
  .pos.updateMarkPrice[sym; px];
 };


// ----------------------------------------------------------------------------
// Fee calculation
// ----------------------------------------------------------------------------

.exec.applyFee: {[grossNotional; isMaker]
  rawBps: $[isMaker; .exec.cfg.makerFeeBps; .exec.cfg.takerFeeBps];
  effBps: rawBps * (100 - .exec.cfg.bnbDiscountPct) % 100;
  grossNotional * effBps % 10000
 };


// ----------------------------------------------------------------------------
// Public: convert an intent into a fill
//
// v0.1 fill semantics:
//   - `buy/`sell at quoted qty: standard market order, signed-qty fill
//   - `flatten: closes the entire open position in the symbol (sign auto-derived)
//   - All fills use taker fees (no limit-order support in v0.1)
// ----------------------------------------------------------------------------

.exec.fill: {[intent]
  sym:    intent `sym;
  action: intent `action;
  reqQty: intent `qty;
  source: intent `source;

  / Need a last price to compute synthetic bid/ask
  lastPx: .exec.state.lastPrice[sym];
  if[null lastPx;
    -1 raze ("EXEC: no price for "; string sym; ", dropping intent");
    :(::)
  ];

  halfSpread: lastPx * .exec.cfg.halfSpreadBps % 10000;
  synBid: lastPx - halfSpread;
  synAsk: lastPx + halfSpread;

  / Resolve flatten -> buy or sell of current open qty
  $[action = `flatten;
    [
      openQty: .pos.qty sym;
      if[0f = openQty;
        / nothing to flatten
        :(::)
      ];
      $[openQty > 0f;
        [action: `sell; reqQty: abs openQty];
        [action: `buy;  reqQty: abs openQty]
      ];
    ];
   action in `buy`sell;
    / no-op: action and reqQty are already in correct form, fall through
    ();
   '"unknown action"
  ];

  / Determine fill price and signed qty
  $[action = `buy;
    [fillPx: synAsk; signedQty: reqQty];
   action = `sell;
    [fillPx: synBid; signedQty: neg reqQty];
   '"impossible"
  ];

  grossNotional: reqQty * fillPx;
  feeUsdt: .exec.applyFee[grossNotional; 0b];   / v0.1: always taker

  `sym`fillPrice`qty`feeUsdt`source ! (sym; fillPx; signedQty; feeUsdt; source)
 };


-1 "execution.q loaded";
