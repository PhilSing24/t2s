\c 50 300

// ============================================================================
// framework.q - Strategy backtest framework v0.1
//
// Core event-loop and dispatch. Loads a strategy file, holds its state, calls
// its callbacks for each market event, routes the resulting intents through
// pretrade -> execution -> position tracking.
//
// FOUR LAYERS (each in its own file):
//   1. Strategy   - emits alpha intents (dispatched by this file)
//   2. Pretrade   - intent gating                       (pretrade.q)
//   3. Execution  - fills, fees, spread                 (execution.q)
//   4. Position   - tracking, funding, auto-exit policy (position.q)
//
// STRATEGY CONTRACT (a strategy file defines these in its own namespace):
//   .strat.<name>.init     [cfg]               -> state                    REQUIRED
//   .strat.<name>.onTrade  [state; tradeEvent] -> (newState; intents)      REQUIRED
//   .strat.<name>.onFill   [state; fillEvent]  -> newState                 optional
//   .strat.<name>.onTimer  [state; ts]         -> (newState; intents)      optional
//   .strat.<name>.onFunding[state; fundingEv]  -> newState                 optional
//
// Intent shape (tagged dict):
//   `action`sym`qty`source ! (`buy|`sell|`flatten; `BTCUSDT; 0.1f; `strategy)
//   source is `strategy or `policy (framework sets `policy for auto-exits)
//
// Multi-symbol is supported architecturally; v0.1 example uses single-symbol.
// ============================================================================


// ----------------------------------------------------------------------------
// Configuration
// ----------------------------------------------------------------------------

.fw.cfg.strategyName:   `;
.fw.cfg.timerIntervalNs: 0;    / if > 0, inject onTimer events at this cadence in virtual time

.fw.state.strategyState:    ()!();
.fw.state.virtualClock:     0Np;
.fw.state.nextTimerTs:      0Np;
.fw.state.eventsProcessed:  0;
.fw.state.intentsGenerated: 0;
.fw.state.intentsApproved:  0;
.fw.state.intentsRejected:  0;
.fw.state.fillsExecuted:    0;


// ----------------------------------------------------------------------------
// Helpers - optional-callback detection
// ----------------------------------------------------------------------------

.fw.fullName: {[cbName] ` sv .fw.cfg.strategyName, cbName};

.fw.hasCallback: {[cbName]
  fullName: .fw.fullName cbName;
  @[{[fn] get fn; 1b}; fullName; {[e] 0b}]
 };


// ----------------------------------------------------------------------------
// Strategy loading
// ----------------------------------------------------------------------------

.fw.loadStrategy: {[strategyFile; namespace; cfg]
  -1 raze ("FW: loading strategy from "; strategyFile);
  system raze ("l "; strategyFile);
  .fw.cfg.strategyName: namespace;
  / Verify required callbacks
  if[not .fw.hasCallback `init;
    '"Strategy must define ", (string namespace), ".init"
  ];
  if[not .fw.hasCallback `onTrade;
    '"Strategy must define ", (string namespace), ".onTrade"
  ];
  / Run init
  initFn: get .fw.fullName `init;
  .fw.state.strategyState: initFn cfg;
  -1 raze ("FW: strategy "; string namespace; " initialized");
 };


// ----------------------------------------------------------------------------
// Intent dispatch pipeline
//
// Every intent (strategy-emitted or policy-emitted) flows through the same
// pipeline: pretrade gate -> execution -> position update. Centralised here
// so both sources go through identical checks.
// ----------------------------------------------------------------------------

.fw.routeIntents: {[intents]
  if[0 = count intents; :()];
  .fw.state.intentsGenerated +: count intents;
  {[intent]
    / 1. Pretrade gate
    pos: .pos.snapshot[];
    decision: .pretrade.check[intent; pos];
    if[not decision `approved;
      .fw.state.intentsRejected +: 1;
      / Per-intent rejection prints would flood the log once the kill switch
      / fires (every subsequent strategy intent gets rejected). The counter
      / above is the source of truth. Summary printed at end of run.
      :()
    ];
    .fw.state.intentsApproved +: 1;
    approvedIntent: decision `intent;

    / 2. Execution -> fill
    fill: .exec.fill[approvedIntent];
    if[(::) ~ fill;
      / execution declined (e.g., no price known yet)
      :()
    ];
    .fw.state.fillsExecuted +: 1;

    / 3. Position update
    .pos.applyFill fill;

    / 4. Optional onFill callback for the strategy
    if[.fw.hasCallback `onFill;
      cb: get .fw.fullName `onFill;
      .fw.state.strategyState: cb[.fw.state.strategyState; fill];
    ];
   } each intents;
 };


// ----------------------------------------------------------------------------
// Virtual clock and timer/funding event injection
//
// Between any two market events, the virtual clock may cross:
//   - timer boundaries (every .fw.cfg.timerIntervalNs)
//   - funding boundaries (00:00, 08:00, 16:00 UTC each day)
// We inject those events in chronological order before processing the next
// market event so the strategy and policies see them in real causal order.
// ----------------------------------------------------------------------------

.fw.advanceClockTo: {[targetTs]
  / inject any virtual events that fire between current clock and targetTs
  / 1. funding events (look up from preloaded data in replay.q)
  fundings: .replay.fundingBetween[.fw.state.virtualClock; targetTs];
  / 2. timer events
  timers: ();
  if[.fw.cfg.timerIntervalNs > 0;
    while[.fw.state.nextTimerTs < targetTs;
      timers: timers, .fw.state.nextTimerTs;
      .fw.state.nextTimerTs +: .fw.cfg.timerIntervalNs;
    ];
  ];
  / merge fundings and timers by timestamp, then dispatch in order
  / fundings is a table; timers is a list of timestamps
  if[count fundings;
    {[r]
      .fw.state.virtualClock: r `fundingTime;
      .fw.dispatchFunding r;
    } each fundings;
  ];
  if[count timers;
    {[t]
      .fw.state.virtualClock: t;
      .fw.dispatchTimer t;
    } each timers;
  ];
  .fw.state.virtualClock: targetTs;
 };

.fw.dispatchFunding: {[fundingEv]
  / 1. Apply funding to positions
  .pos.applyFunding[fundingEv `sym; fundingEv `fundingRate; fundingEv `markPrice];
  / 2. Optional onFunding callback for the strategy
  if[.fw.hasCallback `onFunding;
    cb: get .fw.fullName `onFunding;
    .fw.state.strategyState: cb[.fw.state.strategyState; fundingEv];
  ];
 };

.fw.dispatchTimer: {[ts]
  if[.fw.hasCallback `onTimer;
    cb: get .fw.fullName `onTimer;
    result: cb[.fw.state.strategyState; ts];
    .fw.state.strategyState: result 0;
    .fw.routeIntents result 1;
  ];
  / Check auto-exit policies on every timer tick
  policyIntents: .pos.checkPolicies[];
  if[count policyIntents; .fw.routeIntents policyIntents];
 };


// ----------------------------------------------------------------------------
// Market event entry point
//
// Called by the replay driver (or, in a future live runner, by a CTP
// subscription handler) for each incoming trade event. Updates the virtual
// clock first (injecting any due timer/funding events), then dispatches the
// trade.
// ----------------------------------------------------------------------------

.fw.onTrade: {[trade]
  ts: trade `exchTradeTs;
  if[ts > .fw.state.virtualClock;
    .fw.advanceClockTo ts;
  ];
  / Update execution's last-price tracker
  .exec.recordTrade trade;

  / Dispatch to strategy
  cb: get .fw.fullName `onTrade;
  result: cb[.fw.state.strategyState; trade];
  .fw.state.strategyState: result 0;
  .fw.routeIntents result 1;

  / After the trade has been processed, check auto-exit policies
  / (so e.g. a stop-loss can fire on the very tick that triggered it)
  policyIntents: .pos.checkPolicies[];
  if[count policyIntents; .fw.routeIntents policyIntents];

  .fw.state.eventsProcessed +: 1;
 };


// ----------------------------------------------------------------------------
// Status / reporting
// ----------------------------------------------------------------------------

.fw.status: {[]
  `strategy`virtualClock`events`intentsGen`intentsApp`intentsRej`fills !
  (.fw.cfg.strategyName;
   .fw.state.virtualClock;
   .fw.state.eventsProcessed;
   .fw.state.intentsGenerated;
   .fw.state.intentsApproved;
   .fw.state.intentsRejected;
   .fw.state.fillsExecuted)
 };

-1 "framework.q loaded";
