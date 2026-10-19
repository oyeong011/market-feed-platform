"use strict";
// 서버가 준 문자열은 전부 textContent 로만 넣는다 (innerHTML 금지 — 닉네임·오류문도 외부 입력이다).

const $ = (id) => document.getElementById(id);
const won = (n) => Number(n || 0).toLocaleString("ko-KR");
const pct = (n) => `${n > 0 ? "+" : ""}${Number(n || 0).toFixed(2)}%`;
const ticket = { side: "buy" };
let me = null;
let lastPrice = 0;
let fee = 0.015;
let timers = [];

async function api(path, body) {
  const init = { credentials: "same-origin", headers: {} };
  if (body !== undefined) {
    init.method = "POST";
    init.headers["content-type"] = "application/json";
    init.headers["x-contest-request"] = "1";
    init.body = JSON.stringify(body);
  }
  const r = await fetch(path, init);
  const data = await r.json().catch(() => ({}));
  if (r.status === 401 && !["/api/login", "/api/signup"].includes(path)) { showLogin(); throw new Error("login"); }
  return { status: r.status, data };
}

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}

function signClass(n) { return n > 0 ? "up" : n < 0 ? "down" : ""; }

function showLogin() {
  timers.forEach(clearInterval); timers = [];
  $("app").hidden = true; $("login").hidden = false;
}

function showApp(s) {
  me = s.nickname;
  fee = s.contest.fee_pct;
  $("login").hidden = true; $("app").hidden = false;
  $("me").textContent = me;
  renderStatus(s.contest);
  refreshAll();
  timers = [setInterval(refreshQuote, 3000), setInterval(refreshAccount, 5000),
            setInterval(refreshBoard, 15000), setInterval(refreshSession, 30000)];
}

function renderStatus(c) {
  const b = $("status-badge");
  const halted = c.status.includes("점검");
  b.textContent = halted ? "점검 중" : c.trading ? (c.status === "장외 시험 모드" ? "시험 모드" : "거래 중") : "장 마감";
  b.className = "badge" + (halted ? " halt" : c.trading ? "" : " off");
  const s = $("status");
  s.hidden = c.trading && c.status === "주문 가능";
  s.textContent = c.status;
  $("order-btn").disabled = !c.trading;
}

function refreshAll() { refreshQuote(); refreshAccount(); refreshBoard(); refreshOrders(); }

async function refreshSession() {
  const { data } = await api("/api/session").catch(() => ({}));
  if (data?.contest) renderStatus(data.contest);
}

async function refreshQuote() {
  const sym = $("symbol").value;
  if (!/^\d{6}$/.test(sym) || document.hidden) return;
  const { status, data } = await api(`/api/quote?symbol=${sym}`).catch(() => ({}));
  if (status !== 200) { $("q-price").textContent = "-"; $("q-change").textContent = data?.error || ""; return; }
  lastPrice = data.price;
  $("q-price").textContent = won(data.price);
  const ch = $("q-change");
  ch.textContent = `${data.change > 0 ? "+" : ""}${won(data.change)} (${pct(data.change_pct)})`;
  ch.className = "change " + signClass(data.change);
  updateEstimate();
}

async function refreshAccount() {
  const { status, data } = await api("/api/account").catch(() => ({}));
  if (status !== 200) return;
  $("equity").textContent = won(data.equity) + "원";
  const r = $("ret");
  r.textContent = pct(data.return_pct);
  r.className = "change " + signClass(data.return_pct);
  $("cash").textContent = `예수금 ${won(data.cash)}원`;
  const ul = $("holdings"); ul.replaceChildren();
  if (!data.positions.length) ul.append(el("li", "muted", "보유 종목이 없습니다"));
  for (const p of data.positions) {
    const li = el("li");
    const left = el("div");
    left.append(el("div", "", p.symbol), el("div", "muted small", `${won(p.qty)}주 · 평균 ${won(p.avg_price)}원`));
    const right = el("div", "r");
    right.append(el("div", "", p.price ? `${won(p.price)}원` : "현재가 대기"),
                 el("div", "small " + signClass(p.pnl), `${p.pnl > 0 ? "+" : ""}${won(p.pnl)}원`));
    li.append(left, right);
    li.addEventListener("click", () => { $("symbol").value = p.symbol; refreshQuote(); });
    ul.append(li);
  }
}

async function refreshBoard() {
  const { status, data } = await api("/api/leaderboard").catch(() => ({}));
  if (status !== 200) return;
  const ol = $("board"); ol.replaceChildren();
  for (const r of data.board) {
    const li = el("li", r.nickname === me ? "me" : "");
    const left = el("div");
    left.append(el("span", "rank", `${r.rank}`), document.createTextNode(r.nickname));
    li.append(left, el("div", "r " + signClass(r.return_pct), pct(r.return_pct)));
    ol.append(li);
  }
}

async function refreshOrders() {
  const { status, data } = await api("/api/orders").catch(() => ({}));
  if (status !== 200) return;
  const ul = $("orders"); ul.replaceChildren();
  if (!data.orders.length) ul.append(el("li", "muted", "아직 주문이 없습니다"));
  for (const o of data.orders) {
    const li = el("li");
    const left = el("div");
    left.append(el("div", o.side === "buy" ? "up" : "down", `${o.side === "buy" ? "매수" : "매도"} ${o.symbol} ${won(o.qty)}주`),
                el("div", "muted small", new Date(o.ts * 1000).toLocaleString("ko-KR")));
    const right = el("div", "r");
    right.append(el("span", "state " + o.state, o.state === "FILLED" ? "체결" : "거부"),
                 el("div", "small muted", o.state === "FILLED" ? `${won(o.price)}원` : o.reason || ""));
    li.append(left, right);
    ul.append(li);
  }
}

function updateEstimate() {
  const qty = parseInt($("qty").value, 10) || 0;
  const amt = qty * lastPrice;
  $("est").textContent = lastPrice ? `예상 ${won(amt)}원 + 수수료 약 ${won(Math.floor(amt * fee / 100))}원` : "";
}

function setSide(side) {
  ticket.side = side;
  document.querySelectorAll("[data-side]").forEach((b) => b.classList.toggle("on", b.dataset.side === side));
  const btn = $("order-btn");
  btn.textContent = side === "buy" ? "매수 주문" : "매도 주문";
  btn.className = "big " + side;
}

function newClientId() {
  const a = new Uint8Array(12);
  crypto.getRandomValues(a);
  return "c" + Array.from(a, (x) => x.toString(16).padStart(2, "0")).join("");
}

async function placeOrder() {
  const sym = $("symbol").value, qty = parseInt($("qty").value, 10);
  if (!/^\d{6}$/.test(sym) || !(qty > 0)) return;
  const d = $("confirm");
  $("c-title").textContent = `${ticket.side === "buy" ? "매수" : "매도"} ${sym} ${won(qty)}주`;
  $("c-body").textContent = `시장가로 지금 현재가에 체결됩니다${lastPrice ? ` (약 ${won(lastPrice)}원)` : ""}.`;
  $("c-ok").className = ticket.side;
  d.returnValue = "";
  d.showModal();
  await new Promise((res) => d.addEventListener("close", res, { once: true }));
  if (d.returnValue !== "ok") return;
  const btn = $("order-btn");
  btn.disabled = true;
  // 같은 주문을 다시 보내도 한 건이 되도록 키를 주문마다 하나 만든다(네트워크 재시도 대비)
  const body = { client_order_id: newClientId(), symbol: sym, side: ticket.side, qty };
  const { status, data } = await api("/api/orders", body).catch(() => ({}));
  btn.disabled = false;
  const s = $("status");
  if (status === 201) { s.hidden = true; }
  else { s.hidden = false; s.textContent = data?.order?.reason || data?.error || "주문 실패"; }
  refreshAll();
}

document.addEventListener("DOMContentLoaded", async () => {
  document.querySelectorAll("[data-tab]").forEach((b) => b.addEventListener("click", () => {
    document.querySelectorAll("[data-tab]").forEach((x) => x.classList.toggle("on", x === b));
    $("login-form").hidden = b.dataset.tab !== "login";
    $("signup-form").hidden = b.dataset.tab !== "signup";
    $("login-msg").textContent = "";
  }));
  $("login-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const { status, data } = await api("/api/login", { nickname: $("l-nick").value, password: $("l-pw").value });
    if (status === 200) { $("l-pw").value = ""; return start(); }
    $("login-msg").textContent = status === 429 ? `잠시 잠겼습니다 (${data.retry_after_s}초)` : data.error || "로그인 실패";
  });
  $("signup-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const { status, data } = await api("/api/signup", { invite: $("s-code").value.trim(),
      nickname: $("s-nick").value.trim(), password: $("s-pw").value });
    if (status === 201) { $("s-pw").value = ""; return start(); }
    $("login-msg").textContent = data.error || "참가 실패";
  });
  $("logout").addEventListener("click", async () => { await api("/api/logout", {}).catch(() => {}); showLogin(); });
  $("symbol-form").addEventListener("submit", (e) => { e.preventDefault(); refreshQuote(); });
  document.querySelectorAll("[data-side]").forEach((b) => b.addEventListener("click", () => setSide(b.dataset.side)));
  $("qty").addEventListener("input", updateEstimate);
  $("order-btn").addEventListener("click", placeOrder);
  const code = new URLSearchParams(location.hash.slice(1)).get("invite");
  if (code) { $("s-code").value = code; document.querySelector('[data-tab="signup"]').click(); }
  start();
});

async function start() {
  const { data } = await api("/api/session").catch(() => ({ data: {} }));
  if (data.authenticated) showApp(data); else showLogin();
}
