// === SIGNALEX VMS HELPERS ===

// ── Text and matching helpers ──────────────────────────────────────────────────
// ─── MULTI-FIELD TEXT HELPER ─────────────────────────────────────────────────
function _sigText(s) {
  return [s.title,s.summary,s.ai_summary,s.ingredient_name,s.ingredient_relevance,s.trend_relevance,s.signal_type].filter(Boolean).join(' ').toLowerCase();
}
function _ingMatch(hay, q) {
  const ql = q.toLowerCase();
  if (hay.includes(ql)) return true;
  // partial: split multi-word/slash names and match any meaningful term
  const terms = ql.split(/[\s\/\-]+/).filter(t=>t.length>2);
  return terms.length > 0 && terms.some(t => hay.includes(t));
}
// Strict word-boundary matcher for entity tab — prevents "vitamin d" matching "vitamin b"
function _entityIngMatch(signal, name) {
  const nameL = name.toLowerCase().trim();
  // 1. Exact ingredient_name field match
  if ((signal.ingredient_name || '').toLowerCase().trim() === nameL) return true;
  const hay = _sigText(signal);
  // 2. Exact substring match (catches "vitamin d3" when query is "vitamin d")
  //    followed by word-boundary OR digit (for D3, D2 etc.) but NOT a letter
  //    that would form a different compound (e.g. "vitamin b")
  const escaped = nameL.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  // Match name preceded by start/space/punct and followed by end/space/punct/digit
  const re = new RegExp('(?:^|[\\s,.(\\/\\-])' + escaped + '(?:[\\s,.)\\/\\-\\d]|$)', 'i');
  return re.test(hay);
}

// ─── UNIQUE AI SUMMARIES ──────────────────────────────────────────────────────
// Detect generic/duplicate AI summaries and replace with signal-specific ones
const _GENERIC_PATTERNS = [
  'clinical trial on vitamin b',
  'positive results could substantiate efficacy claims for premium',
  'efficacy claims for premium vms',
  'positive signal for vms',
  'monitor for regulatory implications',
];
function _isGenericWhy(text) {
  if (!text) return true;
  const t = text.toLowerCase();
  return _GENERIC_PATTERNS.some(p => t.includes(p));
}
function _uniqueWhy(s) {
  const raw = s.ai_summary || s.sentiment_reasoning || '';
  if (!_isGenericWhy(raw)) return raw;
  // Generate a unique summary from signal properties
  const src = authLabel(s.authority);
  const ing = s.ingredient_name && s.ingredient_name !== 'unknown' ? s.ingredient_name : '';
  const evType = s.event_type && s.event_type !== 'other' ? s.event_type.replace(/_/g,' ') : 'research signal';
  const sent = (s.sentiment || '').toLowerCase();
  const sev = (s.severity || '').toLowerCase();
  if (sev === 'high') {
    const action = sent === 'negative' ? 'Immediate portfolio review required' : 'Monitor for compliance impact';
    return `${src}: ${evType}${ing ? ' on ' + ing : ''} — ${action}`;
  }
  if (ing) {
    const action = sent === 'positive' ? 'Supports efficacy dossiers and marketing claims'
      : sent === 'negative' ? 'Flag for regulatory risk assessment'
      : 'Track for market and formulation implications';
    return `${src}: ${evType} on ${ing} — ${action}`;
  }
  // Fallback: derive something unique from title prefix
  const titleSnip = (s.title || evType).slice(0, 55).replace(/\s+\S*$/, '');
  const impact = sent === 'positive' ? 'Positive evidence for claims substantiation'
    : sent === 'negative' ? 'Adverse signal requiring compliance monitoring'
    : 'Track for VMS regulatory implications';
  return `${src}: ${titleSnip} — ${impact}`;
}

// ─── KNOWN INGREDIENT TERMS (for title extraction) ─────────────────────────────
const KNOWN_INGREDIENTS = ['ashwagandha', 'turmeric', 'curcumin', 'melatonin', 'probiotic', 'probiotics', 'omega-3', 'omega 3', 'collagen', 'magnesium', 'vitamin d', 'vitamin c', 'vitamin b', 'zinc', 'iron', 'calcium', 'creatine', 'caffeine', 'cbd', 'cannabidiol', 'berberine', 'resveratrol', 'coq10', 'coenzyme q10', 'nmn', 'nad+', 'glutathione', 'quercetin', 'folic acid', 'folate', 'biotin', 'selenium', 'iodine', 'fish oil', 'krill oil', 'spirulina', 'chlorella', 'maca', 'ginseng', 'echinacea', 'valerian', 'chamomile', 'st. john', 'ginkgo', 'ginkgo biloba', 'saw palmetto', 'black cohosh', 'evening primrose', 'milk thistle', 'rhodiola', 'holy basil', 'bacopa', "lion's mane", 'reishi', 'chaga', 'saffron', 'garlic', 'cinnamon', 'ginger', 'beetroot', 'acai', 'elderberry', 'lutein', 'lycopene', 'beta-carotene', 'astaxanthin', 'phosphatidylserine', 'alpha lipoic', 'acetyl-l-carnitine', 'l-carnitine', 'l-theanine', '5-htp', 'tryptophan', 'glucosamine', 'chondroitin', 'msm', 'hyaluronic acid', 'colostrum', 'whey protein', 'pea protein', 'betaine', 'arginine', 'citrulline', 'beet', 'nitrate', 'peptide', 'bpc-157', 'tb-500', 'ghk-cu', 'artri ajo', 'sea buckthorn', 'inositol', 'chromium', 'boron', 'silicon', 'strontium', 'lithium', 'manganese', 'molybdenum', 'copper', 'potassium', 'sodium bicarbonate', 'sodium phosphate', 'beta-alanine', 'hmb', 'cla', 'green tea', 'egcg', 'raspberry ketone', 'glucomannan', 'psyllium', 'inulin', 'fos', 'mos', 'lactulose', 'xylitol', 'stevia', 'monk fruit'];

// ── Known ingredient extraction ────────────────────────────────────────────────
function _extractIngFromTitle(title) {
  const t = (title||'').toLowerCase();
  for (const kw of KNOWN_INGREDIENTS) { if (t.includes(kw)) return kw; }
  return null;
}

// ── Ingredient insight helpers ──────────────────────────────────────────────────
function _computeIngTrend(name) {
  const now=Date.now(),d7=now-7*864e5,d14=now-14*864e5;
  let curr=0,prev=0,high=0,neg=0,pos=0;
  SIGNALS.forEach(s=>{
    if(!_entityIngMatch(s,name)) return;
    const t=new Date(s.scraped_at||s.created_at).getTime();
    if(t>=d7){curr++;if(s.severity==='high')high++;if(s.sentiment==='negative')neg++;if(s.sentiment==='positive')pos++;}
    else if(t>=d14) prev++;
  });
  const chg=curr-prev;
  const conf=curr>=5?'high':curr>=2?'medium':'low';
  const trend=chg>curr*.3?'rising':chg<-curr*.3?'declining':curr>0?'stable':'flat';
  return {curr,prev,high,neg,pos,chg,conf,trend};
}
function _ingWhyMatters(name,total,high,neg,trend) {
  if(trend==='rising'&&high>0) return `${high} high-severity signal${high>1?'s':''} with rising activity — regulatory attention or emerging safety concern.`;
  if(trend==='rising') return `Activity up this week (${total} signals) — emerging market interest or pending scrutiny. Flag for client briefing.`;
  if(high>2) return `${high} high-severity alerts — review labeling and claims for compliance exposure now.`;
  if(total>0&&neg>total*.6) return `Majority negative sentiment (${Math.round(neg/total*100)}%) — monitor for enforcement or claim challenge risk.`;
  return `${total} signal${total>1?'s':''} tracked across regulatory, research and adverse event sources.`;
}
function _ingAction(name,trend,high,neg) {
  if(trend==='rising'&&high>0) return 'Audit product range immediately; check authority enforcement calendar for upcoming decisions.';
  if(trend==='rising') return 'Flag for client briefing; review label claims against recent evidence base.';
  if(high>0) return 'Review high-severity signals; escalate to compliance team if product exposure confirmed.';
  if(neg>2) return 'Monitor claim language carefully; adverse event reports can precede regulatory action.';
  return 'No immediate action required — continue routine monitoring cadence.';
}
