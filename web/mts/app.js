"use strict";
// 서버가 준 문자열은 전부 textContent 로만 넣는다 (innerHTML 금지 — 종목명·오류문도 외부 입력이다).

const $ = (id) => document.getElementById(id);
const won = (n) => Number(n || 0).toLocaleString("ko-KR");
const STATE_KO = { PENDING_SUBMIT: "전송중", ACCEPTED: "접수", PARTIAL: "부분체결", FILLED: "체결",
                   REJECTED: "거부", UNKNOWN: "확인중", NOT_SENT: "미전송" };
const ticket = { side: "buy", kind: "market" };
let lastPrice = 0;
let timers = [];

async function api(path, opts = {}) {
  const init = { credentials: "same-origin", headers: {} };
  if (opts.body !== undefined) {
    init.method = "POST";
    init.headers["content-type"] = "application/json";
    init.headers["x-desk-request"] = "1";
    init.body = JSON.stringify(opts.body);
  }
  const r = await fetch(path, init);
  const data = await r.json().catch(() => ({}));
  if (r.status === 401 && path !== "/api/login") { showLogin(); throw new Error("login"); }
  return { status: r.status, data };
}

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}

function showLogin() {
  timers.forEach(clearInterval); timers = [];
  $("app").hidden = true; $("login").hidden = false; $("pw").focus();
}

async function showApp(s) {
  $("login").hidden = true; $("app").hidden = false;
  const badge = $("env-badge");
  badge.textContent = s.env === "real" ? "실전" : "모의투자";
  badge.classList.toggle("real", s.env === "real");
  $("account").textContent = s.account;
  refreshQuote(); refreshOrders(); refreshBalance(); refreshRecon();
  timers = [setInterval(refreshQuote, 2000), setInterval(refreshOrders, 3000),
            setInterval(refreshRecon, 5000), setInterval(refreshBalance, 10000)];
}

async function refreshQuote() {
  const sym = $("symbol").value;
  if (!/^\d{6}$/.test(sym) || document.hidden) return;
  const { status, data } = await api(`/api/quote?symbol=${sym}`).catch(() => ({}));
  if (status !== 200) { $("q-meta").textContent = data?.error || "시세 조회 실패"; return; }
  lastPrice = data.price;
  $("q-price").textContent = won(data.price);
  const ch = $("q-change");
  ch.textContent = `${data.change > 0 ? "+" : ""}${won(data.change)} (${data.change_pct.toFixed(2)}%)`;
  ch.className = "change " + (data.change > 0 ? "up" : data.change < 0 ? "down" : "");
  $("q-meta").textContent = `${sym} · 시 ${won(data.open)} 고 ${won(data.high)} 저 ${won(data.low)} · 거래량 ${won(data.volume)}`;
  updateEstimate();
}

async function refreshBalance() {
  const { status, data } = await api("/api/balance?fresh=1").catch(() => ({}));
  if (status !== 200) return;
  $("cash").textContent = `예수금 ${won(data.cash)}원 · 평가 ${won(data.total)}원`;
  const ul = $("holdings"); ul.replaceChildren();
  if (!data.holdings.length) ul.append(el("li", "muted", "보유 종목 없음"));
  for (const h of data.holdings) {
    const li = el("li");
    const l = el("div"); l.append(el("div", "", `${h.name} ${h.symbol}`),
                                  el("div", "muted small", `${h.qty}주 · 평균 ${won(Math.round(h.avg_price))}`));
    const r = el("div", "r " + (h.pnl > 0 ? "up" : h.pnl < 0 ? "down" : ""));
    r.append(el("div", "", won(h.price)), el("div", "small", `${won(h.pnl)}원 (${h.pnl_pct.toFixed(2)}%)`));
    li.append(l, r); ul.append(li);
  }
}

async function refreshOrders() {
  const { status, data } = await api("/api/orders?limit=30").catch(() => ({}));
  if (status !== 200) return;
  const ul = $("orders"); ul.replaceChildren();
  const today = data.orders.filter((o) => o.trade_date === data.trade_date);
  if (!today.length) ul.append(el("li", "muted", "오늘 주문 없음"));
  for (const o of today) {
    const li = el("li");
    const l = el("div");
    const t = new Date(o.created_ts * 1000).toLocaleTimeString("ko-KR", { hour12: false });
    l.append(el("div", o.side === "buy" ? "up" : "down",
                `${o.side === "buy" ? "매수" : "매도"} ${o.symbol} ${o.qty}주 ${o.price ? won(o.price) + "원" : "시장가"}`),
             el("div", "muted small", `${t}${o.odno ? " · #" + o.odno : ""}${o.message && o.state !== "FILLED" ? " · " + o.message : ""}`));
    const r = el("div", "r");
    r.append(el("span", "state " + o.state, STATE_KO[o.state] || o.state));
    if (o.filled_qty) r.append(el("div", "small", `${o.filled_qty}주 @ ${won(Math.round(o.filled_amt / o.filled_qty))}`));
    li.append(l, r); ul.append(li);
  }
}

async function refreshRecon() {
  const { status, data } = await api("/api/reconcile").catch(() => ({}));
  if (status !== 200) return;
  const b = $("recon");
  if (!data.breaks?.length) { b.hidden = true; return; }
  b.hidden = false;
  b.textContent = "원장 불일치 " + data.breaks.length + "건: " + data.breaks.map((x) =>
    x.kind === "position_mismatch" ? `${x.symbol} 원장 ${x.ledger_qty}주 / 증권사 ${x.broker_qty}주`
                                   : `${x.symbol} ${x.side || ""} ${x.kind}`).join(", ");
}

function updateEstimate() {
  const qty = parseInt($("qty").value, 10) || 0;
  const unit = ticket.kind === "limit" ? parseInt($("price").value, 10) || 0 : lastPrice;
  $("est").textContent = unit && qty ? `예상 금액 약 ${won(unit * qty)}원${ticket.kind === "market" ? " (현재가 기준)" : ""}` : "";
}

function setSeg(attr, value) {
  document.querySelectorAll(`[data-${attr}]`).forEach((b) => b.classList.toggle("on", b.dataset[attr] === value));
}

function newClientId() {
  const a = new Uint8Array(12); crypto.getRandomValues(a);
  return "w" + Array.from(a, (x) => x.toString(16).padStart(2, "0")).join("");
}

async function placeOrder() {
  const sym = $("symbol").value, qty = parseInt($("qty").value, 10);
  const price = ticket.kind === "limit" ? parseInt($("price").value, 10) : 0;
  if (!/^\d{6}$/.test(sym) || !(qty > 0) || (ticket.kind === "limit" && !(price > 0))) {
    $("est").textContent = "종목코드·수량·가격을 확인하세요"; return;
  }
  const side = ticket.side === "buy" ? "매수" : "매도";
  $("c-title").textContent = `${side} 주문 확인`;
  $("c-body").textContent = `${sym} ${qty}주 ${price ? won(price) + "원 지정가" : "시장가"}` +
    (lastPrice ? ` · 약 ${won((price || lastPrice) * qty)}원` : "");
  $("c-ok").className = ticket.side;
  const dlg = $("confirm");
  dlg.returnValue = "";
  dlg.showModal();
  await new Promise((res) => dlg.addEventListener("close", res, { once: true }));
  if (dlg.returnValue !== "ok") return;

  // 같은 주문 버튼을 두 번 눌러도 주문 ID 가 같으면 서버가 한 건만 만든다.
  const body = { client_order_id: newClientId(), symbol: sym, side: ticket.side, qty, price };
  const btn = $("order-btn"); btn.disabled = true;
  try {
    const { status, data } = await api("/api/orders", { body });
    $("est").textContent = data.error || (data.order ? `${STATE_KO[data.order.state]}: ${data.order.message || ""}` : `HTTP ${status}`);
    refreshOrders();
  } finally { btn.disabled = false; }
}

document.addEventListener("DOMContentLoaded", async () => {
  $("login-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const { status, data } = await api("/api/login", { body: { password: $("pw").value } });
    $("pw").value = "";
    if (status === 200) { const s = await api("/api/session"); showApp(s.data); return; }
    $("login-msg").textContent = status === 429 ? `잠금 중 — ${data.retry_after_s}초 뒤 다시` : "비밀번호가 틀렸습니다";
  });
  $("logout").addEventListener("click", async () => { await api("/api/logout", { body: {} }).catch(() => {}); showLogin(); });
  $("symbol-form").addEventListener("submit", (e) => { e.preventDefault(); refreshQuote(); });
  document.querySelectorAll("[data-side]").forEach((b) => b.addEventListener("click", () => {
    ticket.side = b.dataset.side; setSeg("side", ticket.side);
    const ob = $("order-btn"); ob.className = "big " + ticket.side;
    ob.textContent = ticket.side === "buy" ? "매수 주문" : "매도 주문";
  }));
  document.querySelectorAll("[data-kind]").forEach((b) => b.addEventListener("click", () => {
    ticket.kind = b.dataset.kind; setSeg("kind", ticket.kind);
    $("price-row").hidden = ticket.kind !== "limit";
    if (ticket.kind === "limit" && !$("price").value && lastPrice) $("price").value = lastPrice;
    updateEstimate();
  }));
  ["qty", "price"].forEach((id) => $(id).addEventListener("input", updateEstimate));
  $("order-btn").addEventListener("click", placeOrder);

  const s = await api("/api/session");
  s.data.authenticated ? showApp(s.data) : showLogin();
});
