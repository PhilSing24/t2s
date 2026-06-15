\c 50 300

// ============================================================================
// pretrade.q - Pre-trade check / intent gate (v0.1)
//
// Validates every intent (strategy-emitted or policy-emitted) before it
// becomes a fill. Returns a decision dict:
//   `approved`reason`intent ! (1b; `ok; intent)
//   `approved`reason`intent ! (0b; `oversize; intent)   etc.
//
// On approval, the returned intent may differ from the input (e.g., qty
// reduced to fit within position cap). On rejection, intent is the original
// for debugging.
//
// v0.1 rules:
//   - Max position size per symbol (intent is reduced or rejected if it would
//     push position past the cap)
//   - Kill switch: if .pos.state.killSwitchTriggered, refuse all non-flatten
//     intents (so policy-emitted flatten intents can still close positions)
//
// Future v0.2+ rules to add here, not in strategy code:
//   - portfolio-level gross exposure cap
//   - per-symbol max order size (separate from position cap)
//   - time-of-day trading windows
//   - venue/account checks for live mode
// ============================================================================


// ----------------------------------------------------------------------------
// Configuration
// ----------------------------------------------------------------------------

.pretrade.cfg.maxPosBySymbol: `BTCUSDT`ETHUSDT`SOLUSDT ! 1.0 10.0 200.0;


// ----------------------------------------------------------------------------
// Public: check an intent against current position state
// ----------------------------------------------------------------------------

.pretrade.check: {[intent; positionSnapshot]
  sym:    intent `sym;
  action: intent `action;
  reqQty: intent `qty;

  / Kill switch: only flattens allowed
  if[.pos.state.killSwitchTriggered & (action <> `flatten);
    :`approved`reason`intent ! (0b; `killSwitchActive; intent)
  ];

  / Flatten passes through unchanged - it can only reduce position
  if[action = `flatten;
    :`approved`reason`intent ! (1b; `ok; intent)
  ];

  / Position-size cap
  cap: .pretrade.cfg.maxPosBySymbol[sym];
  if[null cap;
    :`approved`reason`intent ! (0b; `unknownSymbol; intent)
  ];

  curQty: positionSnapshot[sym];
  if[null curQty; curQty: 0f];

  / Projected position after this fill
  proposedQty: $[action = `buy;  curQty + reqQty;
                 action = `sell; curQty - reqQty;
                 0f];

  / Reject if it would push |position| beyond cap
  if[(abs proposedQty) > cap;
    / Reduce the qty so we land exactly at the cap
    allowedQty: $[action = `buy;  cap - curQty;
                  action = `sell; cap + curQty;
                  0f];
    if[allowedQty <= 0f;
      :`approved`reason`intent ! (0b; `atCap; intent)
    ];
    adjustedIntent: intent;
    adjustedIntent[`qty]: allowedQty;
    :`approved`reason`intent ! (1b; `resizedToCap; adjustedIntent)
  ];

  `approved`reason`intent ! (1b; `ok; intent)
 };


-1 "pretrade.q loaded";
