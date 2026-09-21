'use strict';

// ---------- 小工具 ----------
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const rid = (p) => `${p}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

// 只用于展示：接口返回的是字符串小数，下单时原样把用户输入的字符串发出去，不经过浮点数
function num(v, dp = 2) {
  if (v === null || v === undefined || v === '') return '-';
  const n = Number(v);
  if (!Number.isFinite(n)) return String(v);
  let s = n.toFixed(dp);
  if (s.includes('.')) s = s.replace(/0+$/, '').replace(/\.$/, '');
  return s === '-0' ? '0' : s;
}
// 固定小数位数，末尾的0也显示(2666.60，不是2666.6)。num()会把末尾的0去掉，只用在没有固定精度的场合
function fixed(v, dp) {
  if (v === null || v === undefined || v === '') return '-';
  const n = Number(v);
  if (!Number.isFinite(n)) return String(v);
  const t = n.toFixed(dp);
  return t.replace(/^-(0(\.0*)?)$/, '$1');
}
// 价格按合约的价格精度(priceScale)、数量按数量精度(baseCoinScale)显示，取自/contract/list；查不到合约时退回2位/4位
function contractOf(sym) { return (state.contracts || []).find((c) => c.symbol === sym) || null; }
function px(v, sym) { const c = contractOf(sym || state.symbol); return fixed(v, c && c.priceScale != null ? c.priceScale : 2); }
function qty(v, sym) { const c = contractOf(sym || state.symbol); return fixed(v, c && c.baseCoinScale != null ? c.baseCoinScale : 4); }
const signCls = (v) => (Number(v) > 0 ? 'up' : Number(v) < 0 ? 'down' : '');
const fmtTime = (ms) => (ms ? new Date(Number(ms)).toLocaleString('zh-CN', { hour12: false }) : '-');
const fmtClock = (ms) => new Date(Number(ms)).toLocaleTimeString('zh-CN', { hour12: false });

// ---------- 状态 ----------
const state = {
  symbol: 'BTCUSDT',
  contracts: [],
  detail: null,
  uid: null,
  accounts: JSON.parse(localStorage.getItem('sim.accounts') || '[]'),
  interval: '1m',
  action: 'open',
  type: 'limit',
  tab: 'positions',
  ticker: null,
  funding: null,
  depth: { bids: [], asks: [] },
  trades: [],
  klines: [],
  account: null,
  positions: [],
  orders: [],
  ws: null,
  wsReady: false,
  subs: new Set(),
};
const saveAccounts = () => localStorage.setItem('sim.accounts', JSON.stringify(state.accounts));

// ---------- 提示与日志 ----------
function toast(title, detail, isErr) {
  const el = document.createElement('div');
  el.className = 'toast' + (isErr ? ' err' : '');
  el.innerHTML = `<b>${esc(title)}</b>${detail ? esc(detail) : ''}`;
  $('toasts').appendChild(el);
  setTimeout(() => el.remove(), isErr ? 7000 : 3500);
}

const apiLog = [];
function logCall(entry) {
  apiLog.unshift(entry);
  if (apiLog.length > 60) apiLog.pop();
  renderLog();
}
function renderLog() {
  $('apiLog').innerHTML = apiLog.map((e, i) => {
    const ok = e.code === 200;
    return `<div class="entry" data-i="${i}"><div class="head"><span class="muted">${fmtClock(e.t)}</span><b>${esc(e.method)}</b><span>${esc(e.path)}</span>` +
      `<span class="${ok ? 'status-ok' : 'status-err'}">${ok ? 'OK' : esc(e.errCode || e.code)}</span><span class="muted">${e.ms}ms</span></div>` +
      `<pre>${esc(JSON.stringify({ request: e.body, response: e.resp }, null, 2))}</pre></div>`;
  }).join('');
}
$('apiLog').addEventListener('click', (ev) => {
  const entry = ev.target.closest('.entry');
  if (entry) entry.classList.toggle('open');
});

// ---------- 调后端(经代理签名转发) ----------
async function call(prefix, method, path, body, { quiet = false, write = false, ops = false } = {}) {
  const t0 = performance.now();
  let json;
  try {
    const r = await fetch(prefix + path, {
      method,
      // ops=true表示这是运营类接口，代理用ops密钥签名；其余用trade密钥，跟合作方的用法一致
      headers: { 'X-Sim-Client': '1', 'Content-Type': 'application/json', ...(ops ? { 'X-Sim-Scope': 'ops' } : {}) },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    json = await r.json();
  } catch (e) {
    json = { code: 0, errCode: 'network_error', message: String(e) };
  }
  const ms = Math.round(performance.now() - t0);
  if (!quiet) logCall({ t: Date.now(), method, path, body, resp: json, code: json.code, errCode: json.errCode, ms });
  if (write) {
    if (json.code === 200) toast('成功', method + ' ' + path.split('?')[0]);
    else toast(`失败 ${json.errCode || json.code}`, json.message, true);
  }
  return json;
}
const api = (method, path, body, opts) => call('/api', method, path, body, opts);
const engine = (path, opts) => call('/engine', 'GET', path, undefined, opts);
const data = (j, def) => (j && j.code === 200 && j.data !== null && j.data !== undefined ? j.data : def);

// ---------- 弹窗(不用alert/prompt) ----------
function modal(title, fields, okText = '确定') {
  return new Promise((resolve) => {
    const m = $('modal');
    m.innerHTML = `<div class="box"><b>${esc(title)}</b>` +
      fields.map((f, i) => `<label>${esc(f.label)}<input id="mf${i}" value="${esc(f.value ?? '')}" inputmode="${f.inputmode || 'text'}" autocomplete="off"></label>`).join('') +
      `<div class="actions"><button id="mCancel">取消</button><button id="mOk" class="buy">${esc(okText)}</button></div></div>`;
    m.classList.remove('hidden');
    const first = $('mf0');
    if (first) { first.focus(); first.select(); }
    const close = (v) => { m.classList.add('hidden'); m.innerHTML = ''; resolve(v); };
    $('mCancel').onclick = () => close(null);
    $('mOk').onclick = () => close(fields.map((_, i) => $('mf' + i).value.trim()));
    m.onkeydown = (e) => { if (e.key === 'Enter') $('mOk').click(); if (e.key === 'Escape') close(null); };
  });
}

// ---------- 行情 ----------
async function loadContracts() {
  state.contracts = data(await api('GET', '/contract/list', undefined, { quiet: true }), []);
  const sel = $('symbol');
  sel.innerHTML = state.contracts.map((c) => `<option>${esc(c.symbol)}</option>`).join('');
  if (state.contracts.length && !state.contracts.find((c) => c.symbol === state.symbol)) state.symbol = state.contracts[0].symbol;
  sel.value = state.symbol;
}
async function loadDetail() {
  state.detail = data(await api('GET', '/contract/detail?symbol=' + state.symbol, undefined, { quiet: true }), null);
  const tiers = (state.detail && state.detail.tiers) || [];
  const maxLev = tiers.length ? Math.max(...tiers.map((t) => t.maxLeverage)) : '';
  $('levHint').textContent = maxLev ? `最大 ${maxLev}x` : '';
  $('amtUnit').textContent = state.symbol.replace(/USDT$/, '');
  refreshInputHints();
}
async function refreshTicker() {
  state.ticker = data(await api('GET', '/market/ticker?symbol=' + state.symbol, undefined, { quiet: true }), null);
  state.funding = data(await api('GET', '/funding/rate?symbol=' + state.symbol, undefined, { quiet: true }), null);
  renderTicker();
}
function renderTicker() {
  const t = state.ticker || {};
  const f = state.funding || {};
  const chg = t.change24h === null || t.change24h === undefined ? null : Number(t.change24h) * 100;
  const countdown = f.nextFundingTime ? (() => {
    const s = Math.max(0, Math.floor((f.nextFundingTime - Date.now()) / 1000));
    return `${String(Math.floor(s / 3600)).padStart(2, '0')}:${String(Math.floor((s % 3600) / 60)).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`;
  })() : '-';
  $('ticker').innerHTML =
    `<div><span>最新价</span><b class="big ${chg === null ? '' : signCls(chg)}">${px(t.lastPrice)}</b></div>` +
    `<div><span>标记价</span><b>${px(t.markPrice)}</b></div>` +
    `<div><span>指数价</span><b>${px(t.indexPrice)}</b></div>` +
    `<div><span>24h 涨跌</span><b class="${chg === null ? '' : signCls(chg)}">${chg === null ? '-' : (chg > 0 ? '+' : '') + chg.toFixed(2) + '%'}</b></div>` +
    `<div><span>24h 最高/最低</span><b>${px(t.high24h)} / ${px(t.low24h)}</b></div>` +
    `<div><span>24h 成交量</span><b>${num(t.volume24h, 3)}</b></div>` +
    `<div><span>预估资金费率 / 倒计时</span><b>${f.estimatedRate === undefined ? '-' : (Number(f.estimatedRate) * 100).toFixed(4) + '%'} / ${countdown}</b></div>` +
    scenarioBadge();
}
async function refreshDepth() {
  const d = data(await engine('/depth?symbol=' + state.symbol + '&levels=20', { quiet: true }), null);
  if (d) { state.depth = d; renderBook(); }
}
async function refreshTrades() {
  state.trades = data(await api('GET', '/market/trades?symbol=' + state.symbol + '&limit=40', undefined, { quiet: true }), []);
  renderTrades();
}
async function refreshKlines() {
  state.klines = data(await api('GET', `/kline?symbol=${state.symbol}&interval=${state.interval}&limit=120`, undefined, { quiet: true }), []);
  drawChart();
}

function renderBook() {
  const asks = (state.depth.asks || []).slice(0, 12);
  const bids = (state.depth.bids || []).slice(0, 12);
  const max = Math.max(1e-9, ...asks.map((l) => Number(l.volume)), ...bids.map((l) => Number(l.volume)));
  const row = (cls, l) => `<div class="book-row ${cls}" data-price="${esc(l.price)}"><div class="bar" style="width:${(Number(l.volume) / max * 100).toFixed(0)}%"></div>` +
    `<span class="p">${px(l.price)}</span><span class="v">${qty(l.volume)}</span><span class="c">${l.count}</span></div>`;
  const last = state.ticker && state.ticker.lastPrice;
  $('book').innerHTML =
    `<div class="book-head"><span>价格</span><span class="v">数量</span><span class="c">笔数</span></div>` +
    `<div class="book-asks">${asks.slice().reverse().map((l) => row('ask', l)).join('')}</div>` +
    `<div class="book-mid">${last === null || last === undefined ? '-' : px(last)}</div>` +
    `<div class="book-bids">${bids.map((l) => row('bid', l)).join('')}</div>`;
}
$('book').addEventListener('click', (ev) => {
  const row = ev.target.closest('.book-row');
  if (row) { $('fPrice').value = row.dataset.price; updateEst(); } // 直接用接口返回的价格字符串，不经过浮点数
});
function renderTrades() {
  $('recentTrades').innerHTML = state.trades.slice(0, 40).map((t) =>
    `<div class="trade-row"><span class="${t.takerSide === 'buy' ? 'up' : 'down'}">${px(t.price)}</span><span class="v">${qty(t.volume)}</span><span class="c">${fmtClock(t.createTime)}</span></div>`).join('');
}

// ---------- K线(canvas) ----------
function drawChart() {
  const cv = $('chart');
  const dpr = window.devicePixelRatio || 1;
  const w = cv.clientWidth, h = cv.clientHeight;
  if (!w || !h) return;
  cv.width = w * dpr; cv.height = h * dpr;
  const c = cv.getContext('2d');
  c.scale(dpr, dpr);
  c.clearRect(0, 0, w, h);
  const ks = state.klines;
  const padR = 62, padB = 18, volH = 46;
  if (!ks.length) { c.fillStyle = '#848e9c'; c.fillText('还没有K线(这个合约没有成交过)', 16, 24); return; }
  const plotW = w - padR, plotH = h - padB - volH;
  let lo = Infinity, hi = -Infinity, vmax = 0;
  ks.forEach((k) => { lo = Math.min(lo, Number(k.low)); hi = Math.max(hi, Number(k.high)); vmax = Math.max(vmax, Number(k.volume)); });
  if (hi === lo) { hi += 1; lo -= 1; }
  const pad = (hi - lo) * 0.08; hi += pad; lo -= pad;
  const y = (p) => plotH - ((p - lo) / (hi - lo)) * plotH;
  const bw = plotW / ks.length;
  c.font = '11px ui-monospace, monospace';
  c.strokeStyle = '#232a35'; c.fillStyle = '#848e9c'; c.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const p = lo + ((hi - lo) * i) / 4, yy = y(p);
    c.beginPath(); c.moveTo(0, yy); c.lineTo(plotW, yy); c.stroke();
    c.fillText(px(p), plotW + 6, yy + 4);
  }
  ks.forEach((k, i) => {
    const o = Number(k.open), cl = Number(k.close), up = cl >= o;
    const color = up ? '#0ecb81' : '#f6465d';
    const x = i * bw + bw / 2;
    c.strokeStyle = color; c.fillStyle = color;
    c.beginPath(); c.moveTo(x, y(Number(k.high))); c.lineTo(x, y(Number(k.low))); c.stroke();
    const top = y(Math.max(o, cl)), bh = Math.max(1, Math.abs(y(o) - y(cl)));
    c.fillRect(x - Math.max(1, bw * 0.35), top, Math.max(2, bw * 0.7), bh);
    const vh = vmax ? (Number(k.volume) / vmax) * (volH - 6) : 0;
    c.globalAlpha = 0.45; c.fillRect(x - Math.max(1, bw * 0.35), plotH + padB + volH - vh - 2, Math.max(2, bw * 0.7), vh); c.globalAlpha = 1;
  });
  const last = ks[ks.length - 1];
  const ly = y(Number(last.close));
  c.strokeStyle = '#f0b90b'; c.setLineDash([4, 3]);
  c.beginPath(); c.moveTo(0, ly); c.lineTo(plotW, ly); c.stroke(); c.setLineDash([]);
  c.fillStyle = '#f0b90b'; c.fillText(px(last.close), plotW + 6, ly + 4);
  c.fillStyle = '#848e9c';
  const step = Math.max(1, Math.floor(ks.length / 6));
  for (let i = 0; i < ks.length; i += step) c.fillText(new Date(Number(ks[i].openTime)).toLocaleTimeString('zh-CN', { hour12: false, hour: '2-digit', minute: '2-digit' }), i * bw, h - 4);
}
window.addEventListener('resize', drawChart);
function upsertKline(k) {
  const ks = state.klines;
  const i = ks.findIndex((x) => x.openTime === k.openTime);
  if (i >= 0) ks[i] = k;
  else if (!ks.length || k.openTime > ks[ks.length - 1].openTime) { ks.push(k); if (ks.length > 120) ks.shift(); }
  // 最新价 = 当前这一根K线的收盘价。K线来自币安时就是币安的最新价，不用我们自己成交的价格
  // (成交少的时候最后一笔成交价会停很久，跟币安差很多)
  if (state.ticker && ks.length && ks[ks.length - 1].openTime === k.openTime) { state.ticker.lastPrice = k.close; renderTicker(); renderBook(); }
  drawChart();
}
function buildIntervals() {
  $('intervals').innerHTML = ['1m', '5m', '15m', '1h', '4h', '1d'].map((i) => `<button data-i="${i}" class="${i === state.interval ? 'active' : ''}">${i}</button>`).join('');
}
$('intervals').addEventListener('click', (ev) => {
  const b = ev.target.closest('button');
  if (!b) return;
  state.interval = b.dataset.i;
  buildIntervals(); syncSubs(); refreshKlines();
});

// ---------- 账户 ----------
function renderAccountSelect() {
  const sel = $('uidSel');
  sel.innerHTML = state.accounts.length ? state.accounts.map((u) => `<option value="${u}">${u}</option>`).join('') : '<option value="">(还没有账户)</option>';
  if (state.uid) sel.value = String(state.uid);
}
async function refreshAccount() {
  if (!state.uid) { state.account = null; state.positions = []; state.orders = []; renderAccount(); renderTab(); return; }
  const [acc, pos, ord] = await Promise.all([
    api('GET', '/account/info?uid=' + state.uid, undefined, { quiet: true }),
    api('GET', '/position/current?uid=' + state.uid, undefined, { quiet: true }),
    api('GET', '/order/current?uid=' + state.uid, undefined, { quiet: true }),
  ]);
  if (acc.code === 200) state.account = acc.data;
  else if (acc.errCode === 'account_not_found') state.account = null;
  state.positions = data(pos, []);
  state.orders = data(ord, []);
  renderAccount(); renderTab();
}
function renderAccount() {
  const a = state.account;
  if (!a) { $('acctInfo').innerHTML = '<div class="muted" style="grid-column:1/3">选择或新建一个账户</div>'; $('acctStatus').innerHTML = ''; return; }
  $('acctStatus').innerHTML = `<span class="badge ${a.status === 'frozen' ? 'frozen' : ''}">${a.status === 'frozen' ? '已冻结' : '正常'}</span>`;
  const items = [
    ['权益', fixed(a.equity, 4), ''], ['可用余额', fixed(a.available, 4), ''],
    ['仓位保证金', fixed(a.positionMargin, 4), ''], ['挂单冻结', fixed(a.frozenMargin, 4), ''],
    ['未实现盈亏', fixed(a.totalUnrealizedPnl, 4), signCls(a.totalUnrealizedPnl)], ['信用额度', fixed(a.credit, 4), ''],
    ['轮次', a.round, ''], ['投保', a.isInsured ? '是' : '否', ''],
  ];
  $('acctInfo').innerHTML = items.map(([k, v, cls]) => `<div><span>${k}</span><b class="${cls}">${v}</b></div>`).join('');
  $('opsRound').textContent = `当前第 ${a.round} 轮`;
  $('opsInsured').checked = !!a.isInsured;
}
$('uidSel').addEventListener('change', (e) => selectUid(Number(e.target.value)));
async function selectUid(uid) {
  state.uid = uid || null;
  // 切换账户：别让上一个账户的标签页数据、强平提示状态串到这个账户上
  for (const k of Object.keys(tabCache)) delete tabCache[k];
  liqPending = false;
  localStorage.setItem('sim.uid', String(uid || ''));
  syncSubs();
  await refreshAccount();
}
$('btnNewAcct').addEventListener('click', async () => {
  const next = state.accounts.length ? Math.max(...state.accounts) + 1 : 100001;
  const v = await modal('新建账户', [{ label: '账户 uid', value: String(next), inputmode: 'numeric' }, { label: '初始充值 (USDT)', value: '100000', inputmode: 'decimal' }]);
  if (!v) return;
  const uid = Number(v[0]);
  if (!Number.isSafeInteger(uid) || uid <= 0) { toast('uid 不合法', '必须是正整数', true); return; }
  const r = await api('POST', '/account/create', { uid }, { write: true });
  if (r.code !== 200) return;
  if (!state.accounts.includes(uid)) { state.accounts.push(uid); saveAccounts(); }
  renderAccountSelect();
  if (Number(v[1]) > 0) await api('POST', '/account/balance', { uid, amount: v[1], requestId: rid('dep') }, { write: true, ops: true });
  await selectUid(uid); renderAccountSelect();
});
$('btnDeposit').addEventListener('click', async () => {
  if (!state.uid) return toast('先选择账户', '', true);
  const v = await modal('充值 / 扣减', [{ label: '金额 (USDT，负数=扣减)', value: '10000', inputmode: 'decimal' }]);
  if (!v) return;
  await api('POST', '/account/balance', { uid: state.uid, amount: v[0], requestId: rid('dep') }, { write: true, ops: true });
  refreshAccount();
});

// ---------- 下单 ----------
function setAction(a) {
  state.action = a;
  document.querySelectorAll('#actionSeg button').forEach((b) => b.classList.toggle('active', b.dataset.action === a));
  $('btnLong').textContent = a === 'open' ? '买入 / 开多' : '平空 (买入平仓)';
  $('btnShort').textContent = a === 'open' ? '卖出 / 开空' : '平多 (卖出平仓)';
  $('fReduce').checked = a === 'close';
  updateEst();
}
function setType(t) {
  state.type = t;
  document.querySelectorAll('#typeSeg button').forEach((b) => b.classList.toggle('active', b.dataset.type === t));
  $('fPrice').disabled = t === 'market';
  refreshInputHints();
  $('btnLast').disabled = t === 'market';
  updateEst();
}
$('actionSeg').addEventListener('click', (e) => { const b = e.target.closest('button'); if (b) setAction(b.dataset.action); });
$('typeSeg').addEventListener('click', (e) => { const b = e.target.closest('button'); if (b) setType(b.dataset.type); });
$('btnLast').addEventListener('click', () => { if (state.ticker && state.ticker.lastPrice) { $('fPrice').value = px(state.ticker.lastPrice); updateEst(); } });
['fPrice', 'fAmount', 'fLev'].forEach((id) => $(id).addEventListener('input', updateEst));
// ---------- 下单输入按合约规则约束 ----------
// 跟合作方的前端一样：位数、步长、最小/最大量全部取自 /contract/detail(priceScale、baseCoinScale、priceTick、volumeStep、
// minVolume、maxVolume)，不写死。priceTick/volumeStep是0表示服务端不校验，这时输入精度按priceScale/baseCoinScale。
// 十进制字符串按整数运算判断"是不是步长的整数倍"，不经过浮点数，避免0.1+0.2这类误差
function decimalsOf(str) { const t = String(str).replace(/0+$/, ''); const i = t.indexOf('.'); return i < 0 ? 0 : t.length - i - 1; }
// 十进制字符串 -> 放大10^dp的BigInt；不是非负十进制数、或小数位数超过dp(末尾的0不算)返回null
function scaled(str, dp) {
  const m = /^(\d+)(?:\.(\d+))?$/.exec(String(str).trim());
  if (!m) return null;
  const frac = (m[2] || '').replace(/0+$/, '');
  return frac.length > dp ? null : BigInt(m[1] + frac.padEnd(dp, '0'));
}
function unitStr(scale) { return scale > 0 ? '0.' + '0'.repeat(scale - 1) + '1' : '1'; }
// 价格/数量的步长：配置了priceTick/volumeStep就用它，否则是最小的一位小数(10^-scale)
function priceStepOf(d) { return d && Number(d.priceTick) > 0 ? String(d.priceTick) : unitStr(d ? d.priceScale : 2); }
function qtyStepOf(d) { return d && Number(d.volumeStep) > 0 ? String(d.volumeStep) : unitStr(d ? d.baseCoinScale : 3); }
// 校验一个数是不是符合位数和步长；返回错误说明，合法返回空串
function checkStep(label, str, scale, step) {
  const dp = Math.max(scale, decimalsOf(step));
  if (scaled(str, scale) === null) return Number.isNaN(Number(str)) ? `${label}不合法` : `${label}最多${scale}位小数`;
  const v = scaled(str, dp), st = scaled(step, dp);
  if (v === null || st === null || st === 0n) return `${label}不合法`;
  return v % st === 0n ? '' : `${label}必须是${step}的整数倍`;
}
// 下单前的输入校验(不发请求)。price传null表示市价单。d是/contract/detail的返回，没有就不校验(交给服务端)
function validateOrderInput(d, amount, price) {
  if (!d) return '';
  let err = checkStep('数量', amount, d.baseCoinScale, qtyStepOf(d));
  if (err) return err;
  const dp = Math.max(d.baseCoinScale, decimalsOf(d.minVolume), decimalsOf(d.maxVolume));
  const v = scaled(amount, dp);
  if (Number(d.minVolume) > 0 && v < scaled(d.minVolume, dp)) return `数量不能小于最小下单量${d.minVolume}`;
  if (Number(d.maxVolume) > 0 && v > scaled(d.maxVolume, dp)) return `数量不能超过单笔上限${d.maxVolume}`;
  if (price !== null) {
    err = checkStep('价格', price, d.priceScale, priceStepOf(d));
    if (err) return err;
  }
  return '';
}
// 数量向下取到步长的整数倍，返回固定位数的字符串(百分比按钮用)
function floorQty(amt, d) {
  const step = Number(qtyStepOf(d));
  const n = Math.floor(amt / step + 1e-9);
  return fixed(n * step, d ? d.baseCoinScale : 3);
}
// 输入框的提示：步长和最小量
function refreshInputHints() {
  const d = state.detail;
  $('fAmount').placeholder = d ? `步长 ${qtyStepOf(d)} · 最小 ${d.minVolume}` : '';
  $('fPrice').placeholder = state.type === 'market' ? '按对手盘成交' : (d ? `步长 ${priceStepOf(d)}` : '');
}
function refPrice() {
  if (state.type === 'limit' && Number($('fPrice').value) > 0) return Number($('fPrice').value);
  const t = state.ticker || {};
  return Number(t.markPrice || t.lastPrice || t.indexPrice || 0);
}
function updateEst() {
  const amt = Number($('fAmount').value), lev = Number($('fLev').value), p = refPrice();
  if (!(amt > 0) || !(p > 0)) { $('est').textContent = ''; return; }
  const notional = amt * p;
  const taker = state.detail ? Number(state.detail.takerFee) : 0.0005;
  $('est').innerHTML = `名义价值 ${fixed(notional, 2)} USDT<br>` +
    (state.action === 'open' && lev > 0 ? `所需保证金 ≈ ${fixed(notional / lev, 2)} USDT<br>` : '') + `taker 手续费 ≈ ${fixed(notional * taker, 4)} USDT`;
}
function buildPct() {
  $('pct').innerHTML = [25, 50, 75, 100].map((p) => `<button type="button" class="mini" data-p="${p}">${p}%</button>`).join('');
}
$('pct').addEventListener('click', (e) => {
  const b = e.target.closest('button');
  if (!b || !state.account) return;
  const p = Number(b.dataset.p) / 100, lev = Number($('fLev').value) || 1, price = refPrice();
  if (!(price > 0)) return;
  let amt;
  if (state.action === 'open') amt = (Number(state.account.available) * lev * p) / price;
  else { const pos = state.positions.filter((x) => x.symbol === state.symbol && Number(x.volume) > 0); amt = pos.length ? Number(pos[0].volume) * p : 0; }
  $('fAmount').value = floorQty(amt, state.detail); updateEst();
});

async function placeOrder(side) {
  if (!state.uid) return toast('先选择账户', '', true);
  const amount = $('fAmount').value.trim();
  if (!(Number(amount) > 0)) return toast('数量不合法', '', true);
  const body = { uid: state.uid, symbol: state.symbol, side, action: state.action, type: state.type, amount, leverage: Number($('fLev').value) || 1, reduceOnly: $('fReduce').checked, requestId: rid('ord') };
  if (state.type === 'limit') {
    const price = $('fPrice').value.trim();
    if (!(Number(price) > 0)) return toast('价格不合法', '', true);
    body.price = price;
  }
  const bad = validateOrderInput(state.detail, amount, state.type === 'limit' ? body.price : null);
  if (bad) return toast('不符合合约规则', bad, true);
  await api('POST', '/order/add', body, { write: true });
  refreshAccount();
}
// 开仓: 买入=开多(long)、卖出=开空(short)；平仓: 买入=平空(short)、卖出=平多(long)
$('btnLong').addEventListener('click', () => placeOrder(state.action === 'open' ? 'long' : 'short'));
$('btnShort').addEventListener('click', () => placeOrder(state.action === 'open' ? 'short' : 'long'));

// ---------- 标签页 ----------
const sideAction = (o) => `<span class="${o.side === 'long' ? 'up' : 'down'}">${o.action === 'open' ? '开' : '平'}${o.side === 'long' ? '多' : '空'}</span>`;
// 我在这笔成交里是买方还是卖方，是不是挂单方(maker)：makerOrderId等于买单号说明买方是maker
function roleOf(t) {
  const isBuyer = Number(t.buyUid) === Number(state.uid);
  const buyerIsMaker = t.makerOrderId === t.buyOrderId;
  const maker = isBuyer ? buyerIsMaker : !buyerIsMaker;
  return (isBuyer ? '买方' : '卖方') + (maker ? ' · maker' : ' · taker');
}
const cols = {
  positions: [['合约', (p) => p.symbol], ['方向', (p) => `<span class="${p.side === 'long' ? 'up' : 'down'}">${p.side === 'long' ? '多' : '空'}</span>`], ['数量', (p) => qty(p.volume, p.symbol), 1], ['开仓均价', (p) => px(p.avgEntryPrice, p.symbol), 1],
    ['标记价', (p) => px(p.markPrice, p.symbol), 1], ['未实现盈亏(USDT)', (p) => `<span class="${signCls(p.unrealizedPnl)}">${fixed(p.unrealizedPnl, 4)}</span>`, 1], ['回报率', (p) => `<span class="${signCls(p.roe)}">${(Number(p.roe) * 100).toFixed(2)}%</span>`, 1],
    ['保证金(USDT)', (p) => fixed(p.positionMargin, 4), 1], ['杠杆', (p) => p.leverage + 'x', 1], ['强平价(估)', (p) => px(p.liquidationPrice, p.symbol), 1], ['状态', (p) => (p.status === 'liquidating' ? '<span class="down">强平中</span>' : p.status)],
    ['', (p) => `<button class="mini" data-act="closePos" data-sym="${p.symbol}" data-side="${p.side}" data-vol="${p.volume}">市价平仓</button> <button class="mini" data-act="lev" data-sym="${p.symbol}" data-side="${p.side}" data-lev="${p.leverage}">杠杆</button>`]],
  orders: [['委托号', (o) => o.orderId], ['合约', (o) => o.symbol], ['方向', (o) => sideAction(o)], ['类型', (o) => o.type], ['价格', (o) => px(o.price, o.symbol), 1], ['数量', (o) => qty(o.amount, o.symbol), 1],
    ['已成交', (o) => qty(o.tradedAmount, o.symbol), 1], ['冻结保证金(USDT)', (o) => fixed(o.frozenMargin, 4), 1], ['状态', (o) => o.status + (o.liquidation ? ' (强平)' : '')], ['时间', (o) => fmtTime(o.createTime)],
    ['', (o) => `<button class="mini" data-act="cancel" data-id="${o.orderId}">撤单</button>`]],
  conditional: [['委托号', (o) => o.orderId], ['合约', (o) => o.symbol], ['方向', (o) => sideAction(o)], ['触发条件', (o) => `标记价 ${o.triggerDirection === 'gte' ? '≥' : '≤'} ${px(o.triggerPrice, o.symbol)}`], ['类型', (o) => o.type],
    ['价格', (o) => (o.type === 'market' ? '-' : px(o.price, o.symbol)), 1], ['数量', (o) => qty(o.amount, o.symbol), 1], ['冻结保证金(USDT)', (o) => fixed(o.frozenMargin, 4), 1], ['状态', (o) => o.status],
    ['', (o) => (o.status === 'pending' ? `<button class="mini" data-act="cancelCond" data-id="${o.orderId}">撤销</button>` : '')]],
  orderHistory: [['委托号', (o) => o.orderId], ['合约', (o) => o.symbol], ['方向', (o) => sideAction(o)], ['类型', (o) => o.type], ['价格', (o) => px(o.price, o.symbol), 1], ['数量', (o) => qty(o.amount, o.symbol), 1],
    ['已成交', (o) => qty(o.tradedAmount, o.symbol), 1], ['成交均价', (o) => px(o.avgDealPrice, o.symbol), 1], ['状态', (o) => o.status + (o.liquidation ? ' (强平)' : '')], ['时间', (o) => fmtTime(o.createTime)]],
  trades: [['成交号', (t) => t.tradeId], ['合约', (t) => t.symbol], ['价格', (t) => px(t.price, t.symbol), 1], ['数量', (t) => qty(t.volume, t.symbol), 1], ['我的角色', (t) => roleOf(t)], ['时间', (t) => fmtTime(t.createTime)]],
  transactions: [['流水号', (t) => t.id], ['类型', (t) => t.type], ['合约/币种', (t) => t.symbol], ['金额(USDT)', (t) => `<span class="${signCls(t.amount)}">${fixed(t.amount, 6)}</span>`, 1], ['备注(requestId)', (t) => esc(t.requestId || '')], ['时间', (t) => fmtTime(t.createTime)]],
  liquidations: [['委托号', (o) => o.orderId], ['合约', (o) => o.symbol], ['方向', (o) => sideAction(o)], ['委托价', (o) => px(o.price, o.symbol), 1], ['数量', (o) => qty(o.amount, o.symbol), 1], ['已成交', (o) => qty(o.tradedAmount, o.symbol), 1], ['成交均价', (o) => px(o.avgDealPrice, o.symbol), 1], ['状态', (o) => o.status], ['时间', (o) => fmtTime(o.createTime)]],
};
const tabData = { positions: () => state.positions, orders: () => state.orders };

// ---- 标记价变了，在页面上重算持仓和账户的盈亏 ----
// 服务端只在下单/成交/撤单/强平这些事件发生时才推一次持仓和账户，标记价变化不会触发。所以标记价推送到了以后，
// 持仓行的标记价、未实现盈亏、回报率和账户面板的未实现盈亏、权益要在页面上按新的标记价重算，不然要等下一次
// 5秒的REST刷新才动。公式跟服务端一致(model.Position.UnrealizedPnl、PositionService.Views)：
// 多头=(标记价-开仓均价)*数量，空头反过来；回报率=未实现盈亏/仓位保证金；权益的变化量=未实现盈亏总和的变化量。
// 纯函数，直接改传进来的对象，返回有没有变化；下一次服务端推送或REST刷新会用服务端算好的值整体覆盖
function recomputeForMark(positions, account, symbol, markStr) {
  const mark = Number(markStr);
  if (!(mark > 0)) return false;
  let delta = 0;
  let changed = false;
  for (const p of positions || []) {
    if (p.symbol !== symbol || !(Number(p.volume) > 0)) continue;
    const entry = Number(p.avgEntryPrice);
    const vol = Number(p.volume);
    const margin = Number(p.positionMargin);
    const pnl = (p.side === 'long' ? mark - entry : entry - mark) * vol;
    delta += pnl - (Number(p.unrealizedPnl) || 0);
    p.markPrice = markStr;
    p.unrealizedPnl = String(pnl);
    if (margin > 0) p.roe = String(pnl / margin);
    changed = true;
  }
  if (changed && account) {
    account.totalUnrealizedPnl = String((Number(account.totalUnrealizedPnl) || 0) + delta);
    account.equity = String((Number(account.equity) || 0) + delta);
  }
  return changed;
}
// 只更新持仓表里标记价、未实现盈亏、回报率这三格，不重建整张表：重建会把每秒一次的刷新和用户点"市价平仓"抢在一起，点击可能被吞掉
function patchPositionCells() {
  if (state.tab !== 'positions') return;
  const trs = document.querySelectorAll('#tabBody tbody tr');
  const rows = tabData.positions();
  if (trs.length !== rows.length) { renderTab(); return; }
  const def = cols.positions;
  rows.forEach((p, i) => {
    const tds = trs[i].children;
    for (const c of [4, 5, 6]) tds[c].innerHTML = def[c][1](p);
  });
}
const tabCache = {};
const tabPath = {
  conditional: () => `/order/conditional/current?uid=${state.uid}`,
  orderHistory: () => `/order/history?uid=${state.uid}&limit=50`,
  trades: () => `/trade/history?uid=${state.uid}&limit=50`,
  transactions: () => `/account/transactions?uid=${state.uid}&limit=50`,
  liquidations: () => `/liquidation/history?uid=${state.uid}&limit=50`,
};
async function loadTab() {
  if (!state.uid || !tabPath[state.tab]) return;
  tabCache[state.tab] = data(await api('GET', tabPath[state.tab](), undefined, { quiet: true }), []);
  renderTab();
}
let toolsFor = null;
function renderTab() {
  const rows = tabData[state.tab] ? tabData[state.tab]() : tabCache[state.tab] || [];
  const def = cols[state.tab];
  renderTools();
  if (!state.uid) { $('tabBody').innerHTML = '<div class="empty">先选择或新建账户</div>'; return; }
  if (!rows.length) { $('tabBody').innerHTML = '<div class="empty">暂无数据</div>'; return; }
  $('tabBody').innerHTML = `<table><thead><tr>${def.map((c) => `<th${c[2] ? ' style="text-align:right"' : ''}>${c[0]}</th>`).join('')}</tr></thead><tbody>` +
    rows.map((r) => `<tr>${def.map((c) => `<td class="${c[2] ? 'num' : ''}">${c[1](r)}</td>`).join('')}</tr>`).join('') + '</tbody></table>';
}
// 工具栏里有输入框，只在切换标签页时重建，不能每次刷新数据都重建(会把用户正在输入的内容清掉)
function renderTools() {
  if (toolsFor === state.tab) return;
  toolsFor = state.tab;
  const el = $('tabTools');
  if (state.tab === 'orders') el.innerHTML = '<button id="btnCancelAll" class="mini">撤销全部挂单</button>';
  else if (state.tab === 'conditional') {
    el.innerHTML = `<select id="cSide"><option value="long">多</option><option value="short">空</option></select>` +
      `<select id="cAction"><option value="close">平仓(止盈止损)</option><option value="open">开仓</option></select>` +
      `<select id="cDir"><option value="gte">标记价 ≥</option><option value="lte">标记价 ≤</option></select><input id="cTrig" placeholder="触发价">` +
      `<select id="cType"><option value="market">市价</option><option value="limit">限价</option></select><input id="cPrice" placeholder="限价时的委托价"><input id="cAmt" placeholder="数量">` +
      `<button id="btnCond" class="mini">创建条件单</button>`;
  } else el.innerHTML = '';
}
$('tabs').addEventListener('click', (e) => {
  const b = e.target.closest('button');
  if (!b) return;
  state.tab = b.dataset.tab;
  document.querySelectorAll('#tabs button').forEach((x) => x.classList.toggle('active', x === b));
  renderTab(); loadTab();
});
$('tabTools').addEventListener('click', async (e) => {
  if (e.target.id === 'btnCancelAll') { await api('POST', '/order/cancel-all', { uid: state.uid, includeConditional: false }, { write: true }); setTimeout(refreshAccount, 600); }
  if (e.target.id === 'btnCond') {
    const body = { uid: state.uid, symbol: state.symbol, side: $('cSide').value, action: $('cAction').value, triggerDirection: $('cDir').value, triggerPrice: $('cTrig').value.trim(), type: $('cType').value, amount: $('cAmt').value.trim(), leverage: Number($('fLev').value) || 1, reduceOnly: $('cAction').value === 'close', requestId: rid('cond') };
    if ($('cType').value === 'limit') body.price = $('cPrice').value.trim();
    await api('POST', '/order/conditional/add', body, { write: true }); loadTab(); refreshAccount();
  }
});
$('tabBody').addEventListener('click', async (e) => {
  const b = e.target.closest('button[data-act]');
  if (!b) return;
  const act = b.dataset.act;
  if (act === 'cancel') { await api('POST', '/order/cancel/' + b.dataset.id, { uid: state.uid }, { write: true }); setTimeout(refreshAccount, 500); }
  if (act === 'cancelCond') { await api('POST', '/order/conditional/cancel/' + b.dataset.id, { uid: state.uid }, { write: true }); loadTab(); refreshAccount(); }
  if (act === 'closePos') {
    // 市价平仓：平多=卖出(side=long, action=close)，平空=买入(side=short, action=close)，数量=整个仓位
    await api('POST', '/order/add', { uid: state.uid, symbol: b.dataset.sym, side: b.dataset.side, action: 'close', type: 'market', amount: b.dataset.vol, leverage: 1, reduceOnly: true, requestId: rid('close') }, { write: true });
    setTimeout(refreshAccount, 500);
  }
  if (act === 'lev') {
    const v = await modal(`修改 ${b.dataset.sym} ${b.dataset.side === 'long' ? '多' : '空'}仓杠杆`, [{ label: '新杠杆(整数)', value: b.dataset.lev, inputmode: 'numeric' }]);
    if (!v) return;
    await api('POST', '/position/leverage', { uid: state.uid, symbol: b.dataset.sym, side: b.dataset.side, leverage: Number(v[0]) }, { write: true });
    refreshAccount();
  }
});

// ---------- 运营抽屉 ----------
$('btnOps').addEventListener('click', () => $('drawer').classList.toggle('hidden'));
$('btnCloseDrawer').addEventListener('click', () => $('drawer').classList.add('hidden'));
const needUid = () => { if (!state.uid) { toast('先选择账户', '', true); return false; } return true; };
$('opsFreeze').addEventListener('click', async () => { if (needUid()) { await api('POST', '/account/status', { uid: state.uid, status: 'frozen', reason: $('opsReason').value }, { write: true, ops: true }); setTimeout(refreshAccount, 600); } });
$('opsUnfreeze').addEventListener('click', async () => { if (needUid()) { await api('POST', '/account/status', { uid: state.uid, status: 'active', reason: $('opsReason').value }, { write: true, ops: true }); refreshAccount(); } });
$('opsGrant').addEventListener('click', async () => { if (needUid()) { await api('POST', '/account/credit', { uid: state.uid, amount: $('opsCredit').value.trim(), requestId: rid('credit') }, { write: true, ops: true }); refreshAccount(); } });
$('opsWithdrawBtn').addEventListener('click', async () => { if (needUid()) { await api('POST', '/account/balance', { uid: state.uid, amount: '-' + $('opsWithdraw').value.trim().replace(/^-/, ''), requestId: rid('wd') }, { write: true, ops: true }); refreshAccount(); } });
$('opsInsuredBtn').addEventListener('click', async () => { if (needUid()) { await api('POST', '/account/insured', { uid: state.uid, insured: $('opsInsured').checked }, { write: true, ops: true }); refreshAccount(); } });
$('opsRoundClose').addEventListener('click', async () => { if (needUid() && state.account) { await api('POST', '/account/round/close', { uid: state.uid, round: state.account.round }, { write: true }); setTimeout(refreshAccount, 1500); } });
$('opsIndexBtn').addEventListener('click', async () => { await api('POST', '/index-price', { symbol: state.symbol, price: $('opsIndex').value.trim() }, { write: true, ops: true }); refreshTicker(); });

// ---------- 系统做市(币安行情) ----------
const maker = { available: false, status: null };
// 当前合约的行情相对币安的偏移(百分点)，没有偏移返回0
function currentOffset() {
  const s = maker.status && (maker.status.symbols || []).find((x) => x.symbol === state.symbol);
  return s ? Number(s.offsetPct) : 0;
}
function scenarioBadge() {
  const off = currentOffset();
  return off ? `<div><span>行情情景</span><b class="offset">${off > 0 ? '+' : ''}${num(off, 2)}% 偏移</b></div>` : '';
}
async function refreshMaker() {
  let r;
  try { r = await (await fetch('/sim/maker', { headers: { 'X-Sim-Client': '1' } })).json(); } catch { return; }
  maker.available = !!r.available; maker.status = r.status || null;
  renderMaker();
}
function renderMaker() {
  const pill = $('makerPill'), box = $('makerBox'), toggle = $('makerToggle');
  if (!maker.available) { pill.textContent = '做市: 不可用'; pill.classList.add('off'); box.innerHTML = '<span class="muted">没有配置币安行情地址(-binance-url)</span>'; toggle.disabled = true; $('makerReset').disabled = true; return; }
  const s = maker.status || {};
  pill.textContent = s.enabled ? '做市: 开' : '做市: 关';
  pill.classList.toggle('off', !s.enabled);
  toggle.textContent = s.enabled ? '关闭做市' : '开启做市';
  const lines = (s.symbols || []).map((x) => `${x.symbol} 币安 ${num(x.binanceBid, 2)}/${num(x.binanceAsk, 2)} → 我们 ${num(x.ourBestBid, 2)}/${num(x.ourBestAsk, 2)} · 挂单 ${x.liveBids}买 ${x.liveAsks}卖${x.lastPrintPrice ? ' · 对敲价 ' + num(x.lastPrintPrice, 2) : ''}`);
  const err = s.lastError ? `<div class="err">最近错误 (${fmtClock(s.errorAt)}): ${esc(s.lastError)}</div>` : '';
  renderScenario();
  box.innerHTML = (s.enabled ? '' : '<div class="muted">已关闭：系统在订单簿里的挂单已撤掉</div>') + lines.map(esc).join('<br>') + (s.enabled ? `<div class="muted">已刷新 ${s.cycles} 轮 · 系统账户 uid ${(s.uids || []).join(' / ')}</div>` : '') + err;
}
function renderScenario() {
  const s = maker.status || {};
  const box = $('scenarioBox');
  if (!s.enabled) { box.innerHTML = '<span class="muted">需要先开启做市</span>'; return; }
  const target = Number(s.targetPct || 0);
  const rows = (s.symbols || []).map((x) => `${x.symbol} 当前偏移 ${num(x.offsetPct, 2)}%`);
  const moving = (s.symbols || []).some((x) => Number(x.offsetPct) !== target);
  box.innerHTML = `目标偏移 <b>${target > 0 ? '+' : ''}${num(target, 2)}%</b>${moving ? ' <span class="muted">(推进中，每2秒最多1个百分点)</span>' : ''}<br>${rows.map(esc).join('<br>')}`;
}
async function setOffset(pct) {
  const r = await (await fetch('/sim/maker/offset?pct=' + encodeURIComponent(pct), { method: 'POST', headers: { 'X-Sim-Client': '1' } })).json();
  if (r.code && r.code !== 200) toast('设置失败', r.message, true);
  else toast('行情情景', Number(pct) === 0 ? '回到币安价格(逐步推进)' : `目标偏移 ${pct}% (逐步推进)`);
  refreshMaker();
}
$('scnBtns').addEventListener('click', (e) => { const b = e.target.closest('button'); if (b) setOffset(b.dataset.pct); });
$('scnBtn').addEventListener('click', () => { const v = $('scnPct').value.trim(); if (v) setOffset(v); });
async function makerAction(name) {
  const r = await (await fetch('/sim/maker/' + name, { method: 'POST', headers: { 'X-Sim-Client': '1' } })).json();
  if (r.code && r.code !== 200) toast('操作失败', r.message, true);
  else toast('成功', { start: '做市已开启', stop: '做市已关闭，挂单已撤', reset: '系统账户已重置' }[name]);
  refreshMaker();
}
$('makerToggle').addEventListener('click', () => makerAction(maker.status && maker.status.enabled ? 'stop' : 'start'));
$('makerReset').addEventListener('click', () => makerAction('reset'));
$('makerPill').addEventListener('click', () => $('drawer').classList.remove('hidden'));

// ---------- WebSocket ----------
function wantedChannels() {
  const s = new Set([`depth:${state.symbol}`, `trade:${state.symbol}`, `kline:${state.symbol}:${state.interval}`, `markprice:${state.symbol}`]);
  if (state.uid) s.add(`user:${state.uid}`);
  return s;
}
function syncSubs() {
  if (!state.wsReady) return;
  const want = wantedChannels();
  const add = [...want].filter((c) => !state.subs.has(c));
  const del = [...state.subs].filter((c) => !want.has(c));
  if (del.length) state.ws.send(JSON.stringify({ op: 'unsubscribe', channels: del }));
  if (add.length) state.ws.send(JSON.stringify({ op: 'subscribe', channels: add }));
  state.subs = want;
}
let wsBackoff = 1000;
function connectWS() {
  const ws = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/ws`);
  state.ws = ws;
  let openedAt = 0;
  ws.onopen = () => { openedAt = Date.now(); state.wsReady = true; state.subs = new Set(); setWsStatus(true); syncSubs(); };
  ws.onclose = () => {
    state.wsReady = false; state.subs = new Set(); setWsStatus(false);
    // 连上之后很快又断开(后端在反复重启)不重置退避，撑过5秒才算真的连稳了
    if (openedAt && Date.now() - openedAt > 5000) wsBackoff = 1000;
    setTimeout(connectWS, wsBackoff); wsBackoff = Math.min(wsBackoff * 2, 10000);
  };
  ws.onmessage = (ev) => { try { onWsMessage(JSON.parse(ev.data)); } catch (e) { console.error(e); } };
}
function setWsStatus(ok) {
  $('wsDot').classList.toggle('off', !ok);
  $('wsText').textContent = ok ? 'WS 已连接' : 'WS 未连接(重连中)';
}
function onWsMessage(m) {
  const ch = m.channel || '';
  if (ch === 'sim:error') return toast('WebSocket 错误', m.data, true);
  // 切换合约/周期/账户之后，还在路上的旧频道消息不能画到新的图表上
  if (!wantedChannels().has(ch)) return;
  if (ch.startsWith('depth:')) { state.depth = m.data; renderBook(); }
  else if (ch.startsWith('trade:')) { state.trades.unshift(m.data); state.trades = state.trades.slice(0, 40); renderTrades(); }
  else if (ch.startsWith('kline:')) upsertKline(m.data);
  else if (ch.startsWith('markprice:')) {
    if (state.ticker) { state.ticker.markPrice = m.data.price; renderTicker(); }
    if (recomputeForMark(state.positions, state.account, ch.slice('markprice:'.length), m.data.price)) { renderAccount(); patchPositionCells(); }
  }
  else if (ch.startsWith('user:')) {
    const s = m.data || {};
    if (s.account) state.account = s.account;
    state.positions = s.positions || []; state.orders = s.activeOrders || [];
    noticeLiquidation();
    renderAccount(); renderTab();
  }
}

// 强平提醒：仓位进入强平(status=liquidating)时提示一次，强平完成(仓位没了)时再提示一次
let liqPending = false;
function noticeLiquidation() {
  const liquidating = state.positions.some((p) => p.status === 'liquidating') || state.orders.some((o) => o.liquidation);
  if (liquidating && !liqPending) { liqPending = true; toast('仓位进入强平', '风控判定账户权益 ≤ 维持保证金，系统正在按保护价强平', true); }
  if (!liquidating && liqPending && !state.positions.some((p) => Number(p.volume) > 0)) {
    liqPending = false; toast('强平完成', "看'强平记录'和'资金流水'标签页里的明细", true); tabCache.liquidations = undefined; loadTab();
  }
}

// ---------- 切换合约 ----------
$('symbol').addEventListener('change', async (e) => { state.symbol = e.target.value; await loadSymbol(); });
async function loadSymbol() {
  state.depth = { bids: [], asks: [] }; state.trades = []; state.klines = [];
  renderBook(); renderTrades(); drawChart(); syncSubs();
  await Promise.all([loadDetail(), refreshTicker(), refreshDepth(), refreshTrades(), refreshKlines()]);
  renderBook(); updateEst(); refreshAccount();
}

// ---------- 启动 ----------
async function init() {
  buildIntervals(); buildPct(); setAction('open'); setType('limit'); renderLog();
  await loadContracts();
  renderAccountSelect();
  const saved = Number(localStorage.getItem('sim.uid'));
  if (saved && state.accounts.includes(saved)) state.uid = saved;
  else if (state.accounts.length) state.uid = state.accounts[0];
  renderAccountSelect();
  connectWS();
  await loadSymbol();
  // WS不保证绝对不丢消息，定期用REST校准(接口文档的建议)；WS断了的时候靠它兜底
  refreshMaker();
  setInterval(refreshMaker, 3000);
  setInterval(() => { refreshAccount(); refreshTicker(); if (!state.wsReady) { refreshDepth(); refreshTrades(); } loadTab(); }, 5000);
  setInterval(renderTicker, 1000);
}
init();
