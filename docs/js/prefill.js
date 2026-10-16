// Prefills model inputs with the latest USDA/WASDE numbers and the Crop Monitor estimate (data/crop.json).
// Only fields still at their shipped defaults are changed, so a user's own edits are never overwritten.
(function(){
  function setv(id,v,d){var e=document.getElementById(id);if(!e||v==null||!isFinite(v))return;
    if(e.dataset.touched)return;e.value=(+v).toFixed(d);e.dispatchEvent(new Event('input',{bubbles:true}));e.dispatchEvent(new Event('change',{bubbles:true}));}
  document.addEventListener('input',function(ev){if(ev.isTrusted&&ev.target&&ev.target.id)ev.target.dataset.touched='1';},true);
  window.BMR_PREFILL=fetch('data/crop.json?'+Date.now()).then(function(r){return r.json();}).then(function(c){
    var W=c.wasde||{},H=c.headline||null,B=H?H.balance:null;
    // our crop (Texas drought model, all cotton) carried through the WASDE balance sheet → ending stocks
    var base=B?B.end_stocks:W.end_stocks, lo=B?B.end_lo:null, hi=B?B.end_hi:null;
    // conditional FV: scenario US stocks
    setv('in-usb',base,2);setv('in-usl',lo,2);setv('in-ush',hi,2);setv('in-evus',base,2);
    // master model: production median / low / high; stocks move one-for-one with the crop (WASDE demand held)
    if(H){setv('in-pmed',H.crop,2);setv('in-plo',H.lo,2);setv('in-phi',H.hi,2);setv('in-dresp',0,2);}
    setv('in-wprod',W.prod,2);
    // spreads
    setv('in-wstk',W.end_stocks,2);setv('in-mstk',base,2);setv('in-bstk',lo,2);
    var note=document.createElement('div');note.style.cssText='font:12px Inter,system-ui,sans-serif;color:#52514e;padding:6px 16px;background:#eef2f7;border-bottom:1px solid #e2e1dc';
    note.textContent=H?('Our crop '+H.crop.toFixed(2)+'m bales (average of drought model '+H.drought.crop.toFixed(2)+'m and ratings model '+(H.ratings?H.ratings.crop.toFixed(2):'–')+'m; ±1σ '+H.lo.toFixed(2)+'–'+H.hi.toFixed(2)+'m) vs '+(W.report||'WASDE')+' '+W.prod.toFixed(2)+
      'm → ending stocks '+B.end_stocks.toFixed(2)+'m (±1σ '+B.end_lo.toFixed(2)+'–'+B.end_hi.toFixed(2)+'m) vs WASDE '+B.wasde_end.toFixed(2)+'m, with WASDE beginning stocks, imports, mill use '+B.domestic.toFixed(2)+'m and exports '+B.exports.toFixed(2)+'m held. Updated '+c.generated+'. Edit any field to override.')
      :('Inputs prefilled from the '+(W.report||'latest')+' WASDE. Edit any field to override.');
    var nav=document.querySelector('.bmr-nav');if(nav)nav.insertAdjacentElement('afterend',note);
    return c;
  }).catch(function(){return null;});
})();
