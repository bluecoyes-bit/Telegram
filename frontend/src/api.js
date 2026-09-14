const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function readToken() {
  const params = new URLSearchParams(window.location.search);
  const fromQuery = params.get("token");
  if (fromQuery) {
    localStorage.setItem("tg_api_token", fromQuery);
    params.delete("token");
    const qs = params.toString();
    window.history.replaceState({}, "", window.location.pathname + (qs ? `?${qs}` : "") + window.location.hash);
    return fromQuery;
  }
  if (window.__WEB_API_TOKEN__) return window.__WEB_API_TOKEN__;
  return localStorage.getItem("tg_api_token") || "";
}

async function refreshToken() {
  try {
    const txt = await fetch("/web-config.js", { cache: "no-store" }).then((r) => r.text());
    const match = txt.match(/window\.__WEB_API_TOKEN__\s*=\s*(.*);/);
    if (match) window.__WEB_API_TOKEN__ = JSON.parse(match[1]);
  } catch {
    /* ignore */
  }
}

async function request(endpoint, options = {}, retries = 4) {
  const token = readToken();
  const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  if (token) headers.Authorization = `Bearer ${token}`;
  let res;
  try {
    res = await fetch(endpoint, { ...options, headers });
  } catch {
    return { status: "error", reason: "Network Error" };
  }
  let data = {};
  try {
    data = await res.json();
  } catch {
    data = {};
  }
  if (res.status === 401) {
    await refreshToken();
    if (retries > 0) return request(endpoint, options, retries - 1);
  }
  if (res.status === 409 && retries > 0) {
    await sleep(retries > 2 ? 2200 : 1400);
    return request(endpoint, options, retries - 1);
  }
  if (!res.ok) {
    return {
      status: "error",
      http_status: res.status,
      reason: data.detail || res.statusText || "Request failed",
      ...data,
    };
  }
  return data;
}

const phoneQueues = new Map();
function serialize(phone, fn) {
  const key = String(phone || "_");
  const next = (phoneQueues.get(key) || Promise.resolve()).then(fn, fn);
  phoneQueues.set(key, next.catch(() => {}));
  return next;
}

export const api = {
  getAccounts: () => request("/api/console/accounts?limit=500"),
  getProfile: (phone) => serialize(phone, () => request(`/api/console/profile/${phone}`)),
  getContacts: (phone) => serialize(phone, () => request(`/api/console/contacts/${phone}?limit=500`)),
  getDialogs: (phone) => serialize(phone, () => request(`/api/console/dialogs/${phone}?limit=500`)),
  getChatHistory: (phone, chatId) =>
    serialize(phone, () => request(`/api/console/chat-history/${phone}/${chatId}?limit=100`)),
  getChatInfo: (phone, chatId) => serialize(phone, () => request(`/api/console/chat-info/${phone}/${chatId}`)),
  getChatMembers: (phone, chatId) => serialize(phone, () => request(`/api/console/chat-members/${phone}/${chatId}`)),
  getChatMedia: (phone, chatId, mediaType) =>
    serialize(phone, () =>
      request(`/api/console/chat-media/${phone}/${chatId}?media_type=${encodeURIComponent(mediaType)}&limit=40`)
    ),
  getChatPhoto: (phone, chatId) => serialize(phone, () => request(`/api/console/chat-photo/${phone}/${chatId}`)),
  sendMessage: (phone, chatId, text, replyTo = null, editId = null) =>
    serialize(phone, () =>
      request("/api/console/send", {
        method: "POST",
        body: JSON.stringify({ phone, chat_id: chatId, message: text, text, reply_to: replyTo, edit_id: editId }),
      })
    ),
  deleteMessage: (phone, chatId, msgId, forEveryone = false) =>
    serialize(phone, () =>
      request("/api/console/delete-message", {
        method: "POST",
        body: JSON.stringify({ phone, chat_id: chatId, msg_id: parseInt(msgId, 10), delete_for_everyone: forEveryone }),
      })
    ),
  forwardMessage: (phone, fromChatId, toChatId, msgId) =>
    serialize(phone, () =>
      request("/api/console/forward", {
        method: "POST",
        body: JSON.stringify({
          phone,
          from_chat_id: fromChatId,
          to_chat_id: toChatId,
          msg_id: parseInt(msgId, 10),
        }),
      })
    ),
  smartRoute: (phone, target) =>
    serialize(phone, () => request("/api/console/smart-route", { method: "POST", body: JSON.stringify({ phone, target }) })),
  joinChat: (phone, chatId) =>
    serialize(phone, () => request("/api/console/join-chat", { method: "POST", body: JSON.stringify({ phone, chat_id: chatId }) })),
  globalSearch: (phone, query) =>
    serialize(phone, () => request(`/api/console/global-search/${phone}?q=${encodeURIComponent(query)}`)),
  getAutomationLogs: () => request("/api/console/automation-logs"),
  massExecute: (target) =>
    request("/api/console/mass-execute", { method: "POST", body: JSON.stringify({ target_channel: target }) }),
  getAnalytics: (phone) => request(`/api/console/analytics/${phone}`),
  getSessionHealth: (phone) => request(`/api/console/health/${phone}`),
  ping: (phone) => request(`/api/console/ping/${phone}`, {}, 0),
};

export function initials(name) {
  if (!name) return "?";
  return String(name).trim().charAt(0).toUpperCase() || "?";
}

export function formatTime(timestamp) {
  if (!timestamp) return "";
  const d = new Date(timestamp * 1000);
  const now = new Date();
  const days = Math.floor((now - d) / 86400000);
  if (days === 0) return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  if (days === 1) return "Yesterday";
  if (days < 7) return d.toLocaleDateString([], { weekday: "short" });
  return d.toLocaleDateString([], { day: "numeric", month: "short" });
}

export function formatDateFull(timestamp) {
  if (!timestamp) return "";
  return new Date(timestamp * 1000).toLocaleDateString([], {
    weekday: "long",
    month: "long",
    day: "numeric",
  });
}

export function dialogType(type) {
  const map = {
    people: "private",
    private: "private",
    personal: "private",
    user: "private",
    groups: "group",
    group: "group",
    supergroup: "group",
    channels: "channel",
    channel: "channel",
    bots: "bot",
    bot: "bot",
    saved: "saved",
    service: "service",
  };
  return map[type] || type || "private";
}

export function matchesFilter(dialog, filter) {
  const type = dialogType(dialog.type);
  if (filter === "all") return true;
  if (filter === "unread") return (dialog.unread_count || 0) > 0;
  if (filter === "personal" || filter === "private") return type === "private";
  if (filter === "groups" || filter === "group") return type === "group";
  if (filter === "channels" || filter === "channel") return type === "channel";
  if (filter === "bots" || filter === "bot") return type === "bot";
  if (filter === "archive") return Boolean(dialog.archived);
  return true;
}

export function normalizeMessage(msg) {
  return {
    id: msg.id,
    text: msg.text || msg.message || "",
    date: msg.date || Math.floor(Date.now() / 1000),
    outgoing: msg.outgoing ?? msg.is_self ?? false,
    status: msg.status || ((msg.outgoing ?? msg.is_self) ? "sent" : "read"),
    reply_to_msg_id: msg.reply_to_msg_id || null,
    reply_to_sender: msg.reply_to_sender || msg.sender_name || "",
    reply_to_text: msg.reply_to_text || "",
    forward_from: msg.forward_from || null,
    media: msg.media || null,
    edited: msg.edited || false,
    reactions: msg.reactions || [],
    poll: msg.poll || null,
  };
}

export function loadTemplates() {
  try {
    return JSON.parse(localStorage.getItem("tg_templates") || "[]");
  } catch {
    return [];
  }
}

export function saveTemplates(list) {
  localStorage.setItem("tg_templates", JSON.stringify(list));
}

export const EMOJIS = {
  emoji: ["😀","😁","😂","🤣","😃","😄","😅","😉","😊","😍","🥰","😘","😎","🤔","😐","🙄","😏","😴","😌","😜","👍","👎","👏","🙌","🙏","❤️","🔥","💯","✨","⭐","🎉","🚀","💪","🏆","💎","☕","🍕","🎁"],
  stickers: ["🔥","💯","🚀","✨","⭐","💪","🎉","❤️","👍","👏","🎯","🏆","💎","🌟","⚡"],
  gifs: ["🎥","🎬","🎭","🎨","🎪","🎤","🎧","🎸","🎮","🎲"],
};
