// Prefills model inputs with the latest USDA/WASDE numbers and the Crop Monitor estimate (data/crop.json).
// Only fields still at their shipped defaults are changed, so a user's own edits are never overwritten.
(function(){
  function setv(id,v,d){var e=document.getElementById(id);if(!e||v==null||!isFinite(v))return;
    if(e.dataset.touched)return;e.value=(+v).toFixed(d);e.dispatchEvent(new Event('input',{bubbles:true}));e.dispatchEvent(new Event('change',{bubbles:true}));}
  document.addEventListener('input',function(ev){if(ev.isTrusted&&ev.target&&ev.target.id)ev.target.dataset.touched='1';},true);
  window.BMR_PREFILL=fetch('data/crop.json?'+Date.now()).then(function(r){return r.json();}).then(function(c){
    var W=c.wasde||{},E=c.estimate||{},u=E.us||{};
    var ourUpland=u.prod, usdaUpland=u.usda_prod;
    var dProd=(ourUpland&&usdaUpland)?(ourUpland-usdaUpland)/1e6:0;     // our crop vs USDA flows to stocks
    var base=W.end_stocks!=null?W.end_stocks+dProd:null;
    // conditional FV
    setv('in-usb',base,2);setv('in-usl',base!=null?base-1.2:null,2);setv('in-ush',base!=null?base+1.1:null,2);setv('in-evus',base,2);
    // master model: all-cotton production (our upland estimate + USDA Pima)
    var allUSDA=W.prod, ourAll=(W.prod!=null&&ourUpland&&usdaUpland)?W.prod+dProd:null;
    setv('in-pmed',ourAll,2);setv('in-plo',ourAll!=null?ourAll*0.9:null,2);setv('in-phi',ourAll!=null?ourAll*1.1:null,2);setv('in-wprod',allUSDA,2);
    // spreads
    setv('in-wstk',W.end_stocks,2);setv('in-mstk',base,2);
    var note=document.createElement('div');note.style.cssText='font:12px Inter,system-ui,sans-serif;color:#52514e;padding:6px 16px;background:#eef2f7;border-bottom:1px solid #e2e1dc';
    note.textContent='Inputs prefilled: '+(W.report||'WASDE')+' US '+(W.my||'')+' production '+(W.prod!=null?W.prod.toFixed(2):'–')+'m, ending stocks '+(W.end_stocks!=null?W.end_stocks.toFixed(2):'–')+
      'm; Crop Monitor upland estimate '+(ourUpland?(ourUpland/1e6).toFixed(2):'–')+'m vs USDA '+(usdaUpland?(usdaUpland/1e6).toFixed(2):'–')+'m (updated '+c.generated+'). Edit any field to override.';
    var nav=document.querySelector('.bmr-nav');if(nav)nav.insertAdjacentElement('afterend',note);
    return c;
  }).catch(function(){return null;});
})();
