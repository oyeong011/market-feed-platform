"use strict";
// 서버가 준 문자열은 textContent 로만 넣는다.
const $ = (id) => document.getElementById(id);
const won = (n) => Number(n || 0).toLocaleString("ko-KR");
let timer = null;

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
  if (r.status === 401 && path !== "/api/admin/login") { showLogin(); throw new Error("login"); }
  return { status: r.status, data };
}

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}

function showLogin() { clearInterval(timer); $("app").hidden = true; $("login").hidden = false; }

async function showApp() {
  $("login").hidden = true; $("app").hidden = false;
  refresh(); timer = setInterval(refresh, 10000);
}

async function refresh() {
  const { status, data } = await api("/api/admin/status");
  if (status !== 200) return;
  const b = $("badge");
  b.textContent = data.halted ? "주문 정지" : data.trading ? "거래 중" : "장 마감";
  b.className = "badge" + (data.halted ? " halt" : data.trading ? "" : " off");
  const h = $("halt");
  h.hidden = !data.halted;
  h.textContent = data.halted ? `장부 불일치로 주문이 멈췄습니다: ${JSON.stringify(data.breaks)}` : "";
  $("resume").hidden = !data.halted;
  const filled = data.orders.FILLED || 0, rejected = data.orders.REJECTED || 0;
  const checked = data.last_integrity ? new Date(data.last_integrity * 1000).toLocaleTimeString("ko-KR") : "-";
  $("summary").textContent = `참가자 ${data.participants}명 · 체결 ${won(filled)}건 · 거부 ${won(rejected)}건 · `
    + `장부 대사 ${data.breaks.length ? "불일치 " + data.breaks.length + "건" : "일치"} (${checked}) · ${data.status}`;
  const ol = $("board"); ol.replaceChildren();
  for (const r of data.board) {
    const li = el("li");
    li.append(el("span", "", `${r.rank}. ${r.nickname}`), el("span", "r", `${won(r.equity)}원 (${r.return_pct}%)`));
    ol.append(li);
  }
  refreshInvites(); refreshOrders();
}

async function refreshInvites() {
  const { data } = await api("/api/admin/invites");
  const ul = $("invites"); ul.replaceChildren();
  for (const i of data.invites) {
    const link = `${location.origin}/#invite=${i.code}`;
    const li = el("li");
    li.append(el("span", i.nickname ? "muted" : "", i.nickname ? `${i.code} → ${i.nickname}` : link));
    if (!i.nickname) {
      const btn = el("button", "link", "복사");
      btn.addEventListener("click", () => navigator.clipboard?.writeText(link));
      li.append(btn);
    }
    ul.append(li);
  }
}

async function refreshOrders() {
  const { data } = await api("/api/admin/orders");
  const tb = $("orders"); tb.replaceChildren();
  for (const o of data.orders) {
    const tr = el("tr");
    tr.append(el("td", "", new Date(o.ts * 1000).toLocaleTimeString("ko-KR")), el("td", "", o.nickname),
              el("td", o.side === "buy" ? "up" : "down", `${o.side === "buy" ? "매수" : "매도"} ${o.symbol} ${won(o.qty)}`),
              el("td", "r", o.state === "FILLED" ? won(o.price) : "-"),
              el("td", "", o.state === "FILLED" ? "체결" : `거부: ${o.reason || ""}`));
    tb.append(tr);
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  $("login-form").addEventListener("submit", async (e) => {
    e.preventDefault();
    const { status, data } = await api("/api/admin/login", { password: $("pw").value });
    $("pw").value = "";
    if (status === 200) return showApp();
    $("login-msg").textContent = status === 429 ? "잠시 잠겼습니다" : data.error || "로그인 실패";
  });
  $("make").addEventListener("click", async () => {
    await api("/api/admin/invites", { n: parseInt($("n").value, 10) || 1 });
    refreshInvites();
  });
  $("resume").addEventListener("click", async () => {
    const { status, data } = await api("/api/admin/resume", {});
    if (status !== 200) $("halt").textContent = `재개 거부 — 장부가 아직 어긋나 있습니다: ${JSON.stringify(data.breaks)}`;
    refresh();
  });
  const { data } = await api("/api/session").catch(() => ({ data: {} }));
  if (data.admin) showApp(); else showLogin();
});
