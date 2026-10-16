// Shared loan-floor and spread-fair-value inputs for the Spreads, Master and Conditional FV tabs.
// Data: data/market_model.json (built daily from the USDA FSA AWP announcement, ICE prices and WASDE).
var MM = null;
var MM_READY = fetch('data/market_model.json?' + Date.now())
  .then(function (r) { return r.json(); })
  .then(function (j) { MM = j; return j; })
  .catch(function () { return null; });
var MM_MON = { decf: 'DEC', marf: 'MAR', mayf: 'MAY', julf: 'JUL' };

function mmContract(id) {
  if (!MM) return null;
  var mon = MM_MON[id];
  var c = MM.floors.contracts.filter(function (x) { return x.mon === mon; });
  return c.length ? c[0] : null;                         // nearest contract of that month
}
function mmDecWedge() { var c = mmContract('decf'); return c ? c.wedge : null; }

// Floors for one flat series. P.loan, P.wedge (Dec futures − AWP, user-editable), P.stor (¢/lb/mo), P.rate (CCC %/yr).
// Each contract keeps its own basis to the AWP: wedge_k = wedge_Dec(input) + (wedge_k − wedge_Dec)(data).
function mmFloor(id, P) {
  var c = mmContract(id);
  if (!c) return { hard: P.floor, soft: null, contract: '', wedge: P.wedge };
  var dW = c.wedge - (mmDecWedge() || c.wedge);
  var wedge = P.wedge + dW, hard = P.loan + wedge;
  var soft = c.loan_months ? hard + c.loan_months * (P.stor + P.loan * P.rate / 1200) : null;
  return { hard: hard, soft: soft, contract: c.contract, wedge: wedge, months: c.loan_months };
}

// Spread fair value: average of the 45 days before the roll vs US ending stocks, plus a funding-cost term.
function mmSpreadFV(id, st) {
  var s = MM && MM.spreads[id];
  if (!s || !s.fit_funding) return null;
  var f = s.fit_funding;
  return f.a + f.b / Math.max(st, 0.3) + f.c * s.fin_now;
}
function mmFullCarry(id, stor) {
  var s = MM.spreads[id];
  return -(stor + s.back_price * MM.rate.now / 1200) * s.carry_months;
}
// points in the {x,y,yr,m,cy} shape the scatter charts use
function mmSpreadPts(id) {
  var s = MM.spreads[id];
  return s.points.filter(function (p) { return p.complete && p.yr >= 2010; })
    .map(function (p) { return { x: p.x, y: p.y, us: p.x, yr: p.yr, m: 0, cy: +p.my.slice(0, 4), w: p.weight, lbl: p.contracts }; });
}
function mmSpreadFit(id) {
  var s = MM.spreads[id], f = s.fit_funding;
  return { form: 'inv', win: true, f: { r2: f.r2_weighted, sigma: f.sigma, a: f.a, b: f.b, c: f.c } };
}

function mmFloorTable(P) {
  if (!MM) return '';
  var F = MM.floors, ids = ['decf', 'marf', 'mayf', 'julf'];
  var h = '<table style="width:100%;font-size:11px;margin-top:6px;border-collapse:collapse"><tr style="color:#777"><th style="text-align:left">Contract</th><th>Fut − AWP</th><th>LDP trigger</th><th>Loan b/e</th></tr>';
  ids.forEach(function (id) {
    var f = mmFloor(id, P);
    h += '<tr><td style="text-align:left">' + f.contract + '</td><td style="text-align:right">' + f.wedge.toFixed(2) + '</td><td style="text-align:right"><b>' + f.hard.toFixed(2) + '</b></td><td style="text-align:right">' + (f.soft ? f.soft.toFixed(2) : '—') + '</td></tr>';
  });
  h += '</table><div class="hint" style="margin-top:4px">AWP ' + F.awp.toFixed(2) + '¢ (' + F.effective[0].slice(5) + ' to ' + F.effective[1].slice(5) +
    ') = Far East ' + (F.fe_price || 0).toFixed(2) + ' − ' + F.adjustments.toFixed(2) + ' (cost to market ' + F.cost_to_market.toFixed(2) + ' + SLM 1-1/16" ' + F.slm_adj.toFixed(2) +
    '). Wedge = contract average over the AWP week (' + F.pricing_week[0].slice(5) + ' to ' + F.pricing_week[1].slice(5) + ') − AWP. LDP trigger = loan + wedge: FVs are floored here. ' +
    'Loan b/e = loan cotton entered in early Dec has no equity left by the month before delivery (storage $' + F.storage_bale_month.toFixed(2) + '/bale/mo + CCC interest): soft support.</div>';
  return h;
}

// Inputs follow the latest data unless the viewer has typed over them.
function mmPrefillInputs() {
  if (!MM) return;
  function set(id, v, d) {
    var e = document.getElementById(id);
    if (!e || e.dataset.touched || v == null) return;
    e.value = (+v).toFixed(d);
  }
  set('in-loan', MM.floors.loan, 2);
  set('in-wedge', mmDecWedge(), 2);
  set('in-stor', MM.floors.storage, 3);
  set('in-rate', MM.floors.ccc_rate, 2);
  set('in-q3', 0, 2); set('in-lb', 0, 2); set('in-lbsprd', 0, 2);
}
document.addEventListener('input', function (ev) { if (ev.isTrusted && ev.target && ev.target.id) ev.target.dataset.touched = '1'; }, true);
