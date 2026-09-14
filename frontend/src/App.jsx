import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Message from "./Message.jsx";
import Prism from "./Prism.jsx";
import {
  EMOJIS,
  api,
  dialogType,
  formatDateFull,
  formatTime,
  initials,
  loadTemplates,
  matchesFilter,
  normalizeMessage,
  saveTemplates,
} from "./api";

const FOLDERS = [
  { id: "all", name: "All Chats", filter: "all" },
  { id: "personal", name: "Personal", filter: "private" },
  { id: "groups", name: "Groups", filter: "group" },
  { id: "channels", name: "Channels", filter: "channel" },
  { id: "archive", name: "Archive", filter: "archive" },
];
const SETTINGS_NAV = ["general", "notifications", "privacy", "appearance", "language", "accounts", "templates", "premium", "about"];
const FILTERS = ["all", "unread", "groups", "personal", "channels", "bots", "archive"];
const TERMINAL = new Set(["revoked", "banned", "deactivated", "invalid", "auth_key_duplicated", "permanently_failed", "quarantined"]);

function pickAccount(list) {
  const last = sessionStorage.getItem("tg_selected_phone");
  const usable = list.filter((a) => a.has_session !== false && !TERMINAL.has(String(a.status || "").toLowerCase()));
  const active = usable.filter((a) => String(a.status || "").toLowerCase() === "active");
  if (last && usable.some((a) => a.phone === last)) return last;
  if (last && list.some((a) => a.phone === last)) return last;
  return (active[0] || usable[0] || list[0])?.phone || "";
}
const MASS_TYPES = [
  { id: "scrape", label: "Scrape Members" },
  { id: "add", label: "Add Members" },
  { id: "dm", label: "Send DM" },
  { id: "forward", label: "Batch Forward" },
];

function Avatar({ name, src, className = "avatar" }) {
  if (src) return <img className={className} src={src} alt="" />;
  return <div className={className}>{initials(name)}</div>;
}

function Modal({ open, title, onClose, children, wide }) {
  if (!open) return null;
  return (
    <div className="modal show" onClick={onClose}>
      <div className={`dialog ${wide ? "wide" : ""}`} onClick={(e) => e.stopPropagation()}>
        <div className="dialog-h">
          <h3>{title}</h3>
          <button type="button" onClick={onClose} aria-label="Close">×</button>
        </div>
        <div className="dialog-b">{children}</div>
      </div>
    </div>
  );
}

export default function App() {
  const [accounts, setAccounts] = useState([]);
  const [accountQ, setAccountQ] = useState("");
  const [phone, setPhone] = useState("");
  const [profile, setProfile] = useState(null);
  const [status, setStatus] = useState("disconnected");
  const [dialogs, setDialogs] = useState([]);
  const [loadingDialogs, setLoadingDialogs] = useState(false);
  const [loadingMessages, setLoadingMessages] = useState(false);
  const [filter, setFilter] = useState("all");
  const [query, setQuery] = useState("");
  const [listQuery, setListQuery] = useState("");
  const [chatQuery, setChatQuery] = useState("");
  const [chat, setChat] = useState(null);
  const [messages, setMessages] = useState([]);
  const [historyCache, setHistoryCache] = useState({});
  const [info, setInfo] = useState(null);
  const [members, setMembers] = useState([]);
  const [member, setMember] = useState(null);
  const [draft, setDraft] = useState("");
  const [reply, setReply] = useState(null);
  const [editId, setEditId] = useState(null);
  const [toasts, setToasts] = useState([]);
  const [modal, setModal] = useState(null);
  const [contacts, setContacts] = useState([]);
  const [contactQ, setContactQ] = useState("");
  const [searchHits, setSearchHits] = useState([]);
  const [emojiTab, setEmojiTab] = useState("emoji");
  const [emojiOpen, setEmojiOpen] = useState(false);
  const [infoOpen, setInfoOpen] = useState(false);
  const [railOpen, setRailOpen] = useState(false);
  const [nav, setNav] = useState("chats");
  const [settingsPage, setSettingsPage] = useState("general");
  const [analytics, setAnalytics] = useState({});
  const [massType, setMassType] = useState("scrape");
  const [massTarget, setMassTarget] = useState("");
  const [massLimit, setMassLimit] = useState(50);
  const [massLogs, setMassLogs] = useState("[System Standby] Ready.");
  const [deleteTarget, setDeleteTarget] = useState(null);
  const [deleteEveryone, setDeleteEveryone] = useState(false);
  const [forwardId, setForwardId] = useState(null);
  const [fwdQ, setFwdQ] = useState("");
  const [call, setCall] = useState(null);
  const [infoTab, setInfoTab] = useState("info");
  const [mediaItems, setMediaItems] = useState([]);
  const [viewer, setViewer] = useState(null);
  const [busy, setBusy] = useState(false);
  const [templates, setTemplates] = useState(loadTemplates());
  const [templateDraft, setTemplateDraft] = useState("");
  const gen = useRef(0);
  const threadRef = useRef(null);
  const composerRef = useRef(null);

  const toast = useCallback((message, type = "info") => {
    const id = Date.now() + Math.random();
    setToasts((t) => [...t.slice(-4), { id, message, type }]);
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 2800);
  }, []);

  const loadAccounts = useCallback(async () => {
    const res = await api.getAccounts();
    const list = Array.isArray(res) ? res : res?.accounts || [];
    setAccounts(list);
    if (res?.http_status === 401) toast("API token missing. Open /dashboard/ after the bot starts.", "error");
    if (res?.http_status === 503) toast("WEB_API_TOKEN is not configured on the server", "error");
    return list;
  }, [toast]);

  const selectAccount = useCallback(async (nextPhone) => {
    const my = ++gen.current;
    setPhone(nextPhone);
    sessionStorage.setItem("tg_selected_phone", nextPhone);
    setStatus("connecting");
    setChat(null);
    setMessages([]);
    setDialogs([]);
    setInfo(null);
    setHistoryCache({});
    setLoadingDialogs(true);
    const profileRes = await api.getProfile(nextPhone);
    if (my !== gen.current) return;
    if (profileRes.status === "success") {
      setProfile(profileRes);
      setStatus("connected");
    } else {
      setProfile({ full_name: `Session ${String(nextPhone).slice(-4)}`, phone: nextPhone });
      setStatus(profileRes.http_status === 409 ? "busy" : "disconnected");
      if (profileRes.http_status === 409) toast(profileRes.reason || "Session is busy — retrying chats…", "warning");
    }
    const dialogRes = await api.getDialogs(nextPhone);
    if (my !== gen.current) return;
    setLoadingDialogs(false);
    if (dialogRes.status === "success") {
      setDialogs(dialogRes.dialogs || []);
      setStatus("connected");
    } else {
      setDialogs([]);
      toast(dialogRes.reason || "Could not load chats", "error");
    }
  }, [toast]);

  useEffect(() => {
    let cancelled = false;
    loadAccounts().then((list) => {
      if (cancelled) return;
      const next = pickAccount(list);
      if (next) selectAccount(next);
    });
    return () => { cancelled = true; };
  }, [loadAccounts, selectAccount]);

  useEffect(() => {
    if (!phone) return undefined;
    const t = setInterval(async () => {
      const res = await api.ping(phone);
      if (res.status === "success") setStatus(res.connected ? "connected" : "disconnected");
    }, 60000);
    return () => clearInterval(t);
  }, [phone]);

  useEffect(() => {
    if (modal !== "mass") return undefined;
    const t = setInterval(async () => {
      const res = await api.getAutomationLogs();
      if (res.status === "success" && res.logs) setMassLogs((res.logs || []).join("\n") || "[System Standby] Ready.");
    }, 2000);
    return () => clearInterval(t);
  }, [modal]);

  useEffect(() => {
    if (threadRef.current) threadRef.current.scrollTop = threadRef.current.scrollHeight;
  }, [messages]);

  useEffect(() => {
    const onKey = (e) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        document.getElementById("globalSearch")?.focus();
      }
      if (e.key === "Escape") {
        setModal(null);
        setEmojiOpen(false);
        setViewer(null);
        setMember(null);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const visibleDialogs = useMemo(() => {
    const q = listQuery.toLowerCase();
    return dialogs.filter((d) => {
      if (!matchesFilter(d, filter)) return false;
      if (!q) return true;
      return (d.title || "").toLowerCase().includes(q) || (d.last_message || "").toLowerCase().includes(q);
    });
  }, [dialogs, filter, listQuery]);

  const counts = useMemo(
    () => ({
      all: dialogs.length,
      unread: dialogs.filter((d) => (d.unread_count || 0) > 0).length,
      groups: dialogs.filter((d) => matchesFilter(d, "groups")).length,
      personal: dialogs.filter((d) => matchesFilter(d, "personal")).length,
      channels: dialogs.filter((d) => matchesFilter(d, "channels")).length,
      bots: dialogs.filter((d) => matchesFilter(d, "bots")).length,
      archive: dialogs.filter((d) => matchesFilter(d, "archive")).length,
    }),
    [dialogs]
  );

  const visibleAccounts = useMemo(() => {
    const q = accountQ.toLowerCase();
    if (!q) return accounts;
    return accounts.filter((a) =>
      `${a.first_name || ""} ${a.phone || ""} ${a.username || ""} ${a.status || ""}`.toLowerCase().includes(q)
    );
  }, [accounts, accountQ]);

  const visibleMessages = useMemo(() => {
    const q = chatQuery.toLowerCase();
    const list = messages.map(normalizeMessage);
    if (!q) return list;
    return list.filter((m) => (m.text || "").toLowerCase().includes(q));
  }, [messages, chatQuery]);

  const mediaStats = useMemo(() => {
    const list = messages.map(normalizeMessage);
    return {
      photos: list.filter((m) => m.media?.type === "photo").length,
      videos: list.filter((m) => m.media?.type === "video").length,
      files: list.filter((m) => m.media?.type === "file").length,
      links: list.filter((m) => m.media?.type === "link" || /https?:\/\//.test(m.text || "")).length,
    };
  }, [messages]);

  const openChat = async (item) => {
    if (!phone) return toast("Select a session first", "warning");
    setNav("chats");
    setChat(item);
    setInfo({ title: item.title, type: item.type, status: item.type === "private" ? "online" : item.type });
    setInfoOpen(true);
    setDraft(sessionStorage.getItem(`draft_${item.id}`) || "");
    setChatQuery("");
    setMember(null);
    if (historyCache[item.id]) {
      setMessages(historyCache[item.id]);
    } else {
      setMessages([]);
      setLoadingMessages(true);
      const hist = await api.getChatHistory(phone, item.id);
      const list = hist.status === "success" ? hist.messages || [] : [];
      setMessages(list);
      setHistoryCache((c) => ({ ...c, [item.id]: list }));
      setLoadingMessages(false);
    }
    const infoRes = await api.getChatInfo(phone, item.id);
    if (infoRes.status === "success") {
      const payload = infoRes.info || infoRes;
      setInfo({ ...payload, photo: payload.photo || infoRes.photo });
      if (infoRes.members?.length) setMembers(infoRes.members);
    }
    const isGroup = ["group", "channel"].includes(dialogType(item.type));
    if (isGroup && !(infoRes.status === "success" && infoRes.members?.length)) {
      const mem = await api.getChatMembers(phone, item.id);
      if (mem.status === "success") setMembers(mem.members || []);
    } else if (!isGroup) setMembers([]);
  };

  const reloadHistory = async (chatId) => {
    const hist = await api.getChatHistory(phone, chatId);
    if (hist.status === "success") {
      setMessages(hist.messages || []);
      setHistoryCache((c) => ({ ...c, [chatId]: hist.messages || [] }));
    }
  };

  const send = async (textOverride) => {
    const text = (textOverride ?? draft).trim();
    if (!text || !chat || !phone) return;
    const temp = { id: `tmp_${Date.now()}`, text, date: Math.floor(Date.now() / 1000), outgoing: true, status: "sent", reply_to_msg_id: reply?.id, reply_to_text: reply?.text };
    setMessages((m) => [...m, temp]);
    setDraft("");
    sessionStorage.removeItem(`draft_${chat.id}`);
    const res = await api.sendMessage(phone, chat.id, text, reply?.id, editId);
    setReply(null);
    setEditId(null);
    setEmojiOpen(false);
    if (res.status !== "success") toast("Failed to send message", "error");
    else setTimeout(() => reloadHistory(chat.id), 400);
  };

  const smart = async (target) => {
    if (!phone) return toast("Select a session first", "warning");
    if (!target) return toast("Enter a username, phone or link", "warning");
    const res = await api.smartRoute(phone, target);
    if (res.status === "success") {
      setModal(null);
      openChat({ id: res.chat_id, title: res.title || target, type: "private" });
    } else toast(res.reason || "Route failed", "error");
  };

  const runGlobalSearch = async (value) => {
    const q = (value ?? query).trim();
    if (!q) return;
    if (q.includes("t.me") || q.startsWith("@") || /^\+?\d{8,}$/.test(q)) return smart(q);
    if (!phone) return toast("Select a session first", "warning");
    setModal("search");
    const res = await api.globalSearch(phone, q);
    setSearchHits(res.status === "success" ? res.results || [] : []);
  };

  const openContacts = async (which) => {
    if (!phone) return toast("Select a session first", "warning");
    setNav(which === "contacts" ? "contacts" : nav);
    setModal(which);
    const res = await api.getContacts(phone);
    setContacts(res.status === "success" ? res.contacts || [] : []);
  };

  const openAnalytics = async () => {
    if (!phone) return toast("Select a session first", "warning");
    setNav("broadcast");
    setModal("analytics");
    const [p, h, a] = await Promise.all([api.getProfile(phone), api.getSessionHealth(phone), api.getAnalytics(phone)]);
    setAnalytics({
      profile: p,
      health: h,
      analytics: a,
      total: a.total_sessions || accounts.length,
      active: a.active_sessions || accounts.filter((x) => x.status === "active").length,
      banned: a.banned_sessions || 0,
      flooded: a.flooded_sessions || 0,
    });
  };

  const loadMediaTab = async (tab) => {
    setInfoTab(tab);
    if (!phone || !chat) return;
    const map = { media: "photos", files: "files", links: "links" };
    const kind = map[tab];
    if (!kind) return;
    const res = await api.getChatMedia(phone, chat.id, kind);
    setMediaItems(res.status === "success" ? res.items || [] : []);
  };

  const filteredContacts = contacts.filter((c) => {
    const q = contactQ.toLowerCase();
    const name = `${c.first_name || ""} ${c.last_name || ""}`.toLowerCase();
    return !q || name.includes(q) || (c.username || "").toLowerCase().includes(q) || (c.phone || "").includes(q);
  });

  const activeCount = accounts.filter((a) => String(a.status || "").toLowerCase() === "active").length;
  const operatorName = profile?.full_name || "Choose Account";
  const connected = status === "connected";
  const listTitle = filter === "all" ? "All Chats" : filter[0].toUpperCase() + filter.slice(1);

  return (
    <div className={`app ${chat ? "chat-open" : ""}`}>
      <div className="prism-bg" aria-hidden="true">
        <Prism
          animationType="rotate"
          timeScale={0.45}
          height={3.5}
          baseWidth={5.5}
          scale={3.4}
          hueShift={0.18}
          colorFrequency={1.05}
          noise={0.16}
          glow={0.9}
          bloom={1.2}
          transparent
          lightMode
        />
      </div>
      <div className="toast-wrap">
        {toasts.map((t) => (
          <div key={t.id} className={`toast ${t.type}`}>{t.message}</div>
        ))}
      </div>

      <header className="topbar">
        <button className="icon-btn menu-toggle" type="button" onClick={() => setRailOpen((v) => !v)}>☰</button>
        <div className="brand">
          <div className="mark">TG</div>
          <div>
            <strong>TG Gateway</strong>
            <span>Multi-account Control Center</span>
          </div>
        </div>
        <div className="search">
          <span>⌕</span>
          <input
            id="globalSearch"
            value={query}
            placeholder="Search messages, contacts, groups..."
            onChange={(e) => { setQuery(e.target.value); setListQuery(e.target.value); }}
            onKeyDown={(e) => { if (e.key === "Enter") runGlobalSearch(); }}
          />
          <kbd>Ctrl K</kbd>
        </div>
        <div className="top-actions">
          <button className="icon-btn" type="button" title="Alerts" onClick={() => toast("No new alerts", "info")}>🔔</button>
          <button className="icon-btn" type="button" title="Refresh accounts" onClick={loadAccounts}>⟳</button>
          <div className="operator">
            <Avatar name={operatorName} src={profile?.profile_pic} />
            <div>
              <h3>{operatorName}</h3>
              <p>{phone ? `+${phone}` : "Administrator"}</p>
            </div>
          </div>
        </div>
      </header>

      <div className="workspace">
        <aside className={`pane rail ${railOpen ? "open" : ""}`}>
          <div className="rail-label">ACCOUNTS</div>
          <div className="online-chip">{accounts.length} total · {activeCount} active</div>
          <input className="rail-search" placeholder="Find account" value={accountQ} onChange={(e) => setAccountQ(e.target.value)} />
          <div className="account-list">
            {!accounts.length && <div className="empty tiny">No sessions loaded.</div>}
            {visibleAccounts.map((acc) => (
              <button
                key={acc.phone}
                type="button"
                className={`account-row ${phone === acc.phone ? "active" : ""} ${acc.status || ""}`}
                title={`${acc.first_name || "Account"} +${acc.phone} (${acc.status || "unknown"})`}
                onClick={() => { setRailOpen(false); selectAccount(acc.phone); }}
              >
                <Avatar className="orb" name={acc.first_name || acc.phone} />
                <div className="account-meta">
                  <strong>{acc.first_name || "Account"}</strong>
                  <span>+{acc.phone}</span>
                </div>
                <em className={`st ${acc.status || "pending"}`}>{acc.status || "pending"}</em>
              </button>
            ))}
          </div>
          <button className="rail-add" type="button" onClick={() => openContacts("new")}>+ Add</button>
          <nav className="nav">
            <button className={`nav-btn ${nav === "chats" ? "active" : ""}`} type="button" onClick={() => setNav("chats")}>Chats</button>
            <button className={`nav-btn ${nav === "contacts" ? "active" : ""}`} type="button" onClick={() => openContacts("contacts")}>Contacts</button>
            <button className={`nav-btn ${nav === "groups" ? "active" : ""}`} type="button" onClick={() => { setNav("groups"); setFilter("groups"); }}>Groups</button>
            <button className={`nav-btn ${nav === "broadcast" ? "active" : ""}`} type="button" onClick={openAnalytics}>Broadcast</button>
            <button className={`nav-btn ${nav === "tools" ? "active" : ""}`} type="button" onClick={() => { if (!phone) return toast("Select a session first", "warning"); setNav("tools"); setModal("mass"); }}>Tools</button>
            <button className={`nav-btn ${nav === "settings" ? "active" : ""}`} type="button" onClick={() => { setNav("settings"); setModal("settings"); }}>Settings</button>
            <button className="nav-btn logout" type="button" onClick={() => {
              gen.current += 1;
              setPhone(""); setChat(null); setDialogs([]); setProfile(null); setStatus("disconnected");
              sessionStorage.removeItem("tg_selected_phone");
              toast("Logged out", "info");
            }}>Logout</button>
          </nav>
        </aside>

        <section className="pane list">
          <div className="list-head">
            <h2>{listTitle}</h2>
            <button className="compose" type="button" onClick={() => openContacts("new")} title="New message">✎</button>
          </div>
          <div className="search-mini">
            <span>⌕</span>
            <input placeholder="Filter this list..." value={listQuery} onChange={(e) => setListQuery(e.target.value)} />
          </div>
          <div className="chips">
            {FILTERS.map((f) => (
              <button key={f} className={`chip ${filter === f ? "active" : ""}`} type="button" onClick={() => setFilter(f)}>
                {f[0].toUpperCase() + f.slice(1)}
                {counts[f] != null ? <b>{counts[f]}</b> : null}
              </button>
            ))}
          </div>
          <div className="chats">
            {!phone && <div className="empty">Select an account to load conversations.</div>}
            {phone && loadingDialogs && <div className="empty">Loading conversations…</div>}
            {phone && !loadingDialogs && !visibleDialogs.length && (
              <div className="empty">
                No conversations found.<br />
                Use Add to open a username or join a link.
                <button className="btn primary" type="button" style={{ marginTop: 12 }} onClick={() => selectAccount(phone)}>Retry load</button>
              </div>
            )}
            {visibleDialogs.map((d) => (
              <button key={d.id} type="button" className={`chat-row ${chat?.id === d.id ? "active" : ""}`} onClick={() => openChat(d)}>
                <Avatar className="chat-av" name={d.title} />
                <div className="chat-meta">
                  <div className="chat-top">
                    <strong>{d.title}{d.pinned ? " 📌" : ""}</strong>
                    <span>{formatTime(d.last_date)}</span>
                  </div>
                  <div className="preview">
                    <em className="kind">{d.type}</em>
                    {d.muted ? "🔕 " : ""}{d.archived ? "📁 " : ""}{d.last_message || "No messages"}
                    {d.unread_count > 0 ? <span className="badge">{d.unread_count > 99 ? "99+" : d.unread_count}</span> : null}
                  </div>
                </div>
              </button>
            ))}
          </div>
        </section>

        <main className="pane conversation">
          {!chat ? (
            <div className="placeholder">
              <div className="mark" style={{ margin: "0 auto 12px" }}>TG</div>
              <h2>Select a conversation</h2>
              <p>Pick a session from the rail, then open a chat to message live.</p>
            </div>
          ) : (
            <>
              <div className="header">
                <div className="who" onClick={() => setInfoOpen(true)}>
                  <button className="back" type="button" onClick={(e) => { e.stopPropagation(); setChat(null); }}>←</button>
                  <Avatar className="head-av" name={chat.title} src={info?.photo} />
                  <div>
                    <h3>{chat.title}</h3>
                    <p>{info?.about ? String(info.about).slice(0, 48) : (info?.status || info?.type || "online")}</p>
                  </div>
                </div>
                <div className="actions">
                  <input className="chat-find" placeholder="Find in chat" value={chatQuery} onChange={(e) => setChatQuery(e.target.value)} />
                  <button type="button" onClick={() => setCall({ type: "voice", name: chat.title })}>☎</button>
                  <button type="button" onClick={() => setCall({ type: "video", name: chat.title })}>🎥</button>
                  <button type="button" onClick={() => setInfoOpen(true)}>⋯</button>
                </div>
              </div>
              <div className="thread" ref={threadRef}>
                {loadingMessages ? <div className="empty">Loading messages…</div> : null}
                {visibleMessages.map((msg, i) => {
                  const prev = visibleMessages[i - 1];
                  const day = formatDateFull(msg.date);
                  const showDay = !prev || formatDateFull(prev.date) !== day;
                  return (
                    <div key={msg.id || i}>
                      {showDay ? <div className="date-chip">{day}</div> : null}
                      <Message
                        msg={msg}
                        onReply={(m) => setReply({ id: m.id, text: m.text })}
                        onForward={(m) => { setForwardId(m.id); setModal("forward"); }}
                        onEdit={(m) => { setEditId(m.id); setDraft(m.text || ""); composerRef.current?.focus(); }}
                        onDelete={(m) => { setDeleteTarget(m.id); setModal("delete"); }}
                        onPin={() => toast("Message pinned!", "success")}
                        onRoute={smart}
                        onPhoto={(src, caption) => setViewer({ src, caption })}
                      />
                    </div>
                  );
                })}
              </div>
              {reply || editId ? (
                <div className="reply-bar">
                  <div>
                    <b>{editId ? "Editing" : "Reply"}</b>
                    <span>{reply?.text || draft}</span>
                  </div>
                  <button type="button" className="btn ghost" onClick={() => { setReply(null); setEditId(null); }}>×</button>
                </div>
              ) : null}
              <div className="composer">
                <div className="composer-inner">
                  <button type="button" className="icon-btn" title="Attach" onClick={() => toast("Attachment options: Photo, Video, File, Poll, Location", "info")}>＋</button>
                  <button type="button" className="icon-btn" title="Emoji" onClick={() => setEmojiOpen((v) => !v)}>☺</button>
                  <input
                    ref={composerRef}
                    value={draft}
                    placeholder={`Message ${chat.title}`}
                    onChange={(e) => {
                      setDraft(e.target.value);
                      sessionStorage.setItem(`draft_${chat.id}`, e.target.value);
                    }}
                    onKeyDown={(e) => {
                      if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); }
                    }}
                  />
                  <button type="button" className="icon-btn" title="Voice" onClick={() => toast("Hold to record voice message", "info")}>🎤</button>
                  <button type="button" className="send" onClick={() => send()}>➤</button>
                </div>
              </div>
            </>
          )}
        </main>

        <aside className={`pane info ${infoOpen || chat ? "open" : ""}`}>
          <button className="close-info" type="button" onClick={() => setInfoOpen(false)}>×</button>
          <div className="info-body">
            {member ? (
              <div className="profile">
                <button type="button" className="btn ghost" onClick={() => setMember(null)}>← Back</button>
                <Avatar className="info-av" name={member.name || member.first_name} />
                <h3>{member.name || `${member.first_name || ""} ${member.last_name || ""}`.trim()}</h3>
                <p>{member.username ? `@${member.username}` : "No username"}</p>
                <p>{member.role || member.status || "member"}</p>
                <button className="btn primary" type="button" onClick={() => { setMember(null); openChat({ id: member.id, title: member.name || member.username, type: "private" }); }}>Send Message</button>
              </div>
            ) : (
              <>
                <div className="profile">
                  <Avatar className="info-av" name={info?.title || chat?.title || "Name"} src={info?.photo || info?.profile_pic} />
                  <h3>{info?.title || chat?.title || "Name"}</h3>
                  <div className="phone">{info?.link || info?.phone || info?.username || "Telegram"}</div>
                  <p>{info?.about || info?.status || info?.type || "status"}</p>
                </div>
                <div className="quick-icons">
                  <button type="button" onClick={() => chat && setCall({ type: "voice", name: chat.title })}>Call</button>
                  <button type="button" onClick={() => chat && setCall({ type: "video", name: chat.title })}>Video</button>
                  <button type="button" onClick={() => document.getElementById("globalSearch")?.focus()}>Search</button>
                  <button type="button" onClick={() => setModal("settings")}>More</button>
                </div>
                <div className="tabs">
                  {["info", "media", "files", "links"].map((t) => (
                    <button key={t} type="button" className={infoTab === t ? "active" : ""} onClick={() => loadMediaTab(t)}>{t[0].toUpperCase() + t.slice(1)}</button>
                  ))}
                </div>
                <div className={`live ${connected ? "ok" : ""}`}>
                  <div className="mark" style={{ width: 34, height: 34, fontSize: 11 }}>TG</div>
                  <div>
                    <strong>{status === "connected" ? "Connected & online" : status === "connecting" ? "Connecting..." : status === "busy" ? "Session busy" : "Disconnected"}</strong>
                    <div style={{ fontSize: 11, color: "var(--muted)" }}>Using Telegram session</div>
                  </div>
                  <span className="dot" />
                </div>
                {infoTab === "info" ? (
                  <>
                    <div className="section-title">Quick Actions</div>
                    <button className="qa" type="button" onClick={() => composerRef.current?.focus()}><div><b>Send Message</b><small>Send to this contact</small></div></button>
                    <button className="qa" type="button" onClick={() => phone ? setModal("mass") : toast("Select a session first", "warning")}><div><b>Bulk Message</b><small>Send to multiple chats</small></div></button>
                    <button className="qa" type="button" onClick={() => setModal("templates")}><div><b>Message Templates</b><small>Use saved templates</small></div></button>
                    <button className="qa danger" type="button" onClick={() => toast("Block is not enabled on this session", "warning")}><div><b>Block Contact</b><small>Restrict this chat</small></div></button>
                    <div className="section-title">Shared Media</div>
                    <div className="media-grid">
                      <div>{info?.stats?.photos ?? mediaStats.photos} photos</div>
                      <div>{info?.stats?.videos ?? mediaStats.videos} videos</div>
                      <div>{info?.stats?.files ?? mediaStats.files} files</div>
                      <div>{info?.stats?.links ?? mediaStats.links} links</div>
                    </div>
                    <div className="section-title">Members {info?.member_count || members.length}</div>
                    {members.map((m) => (
                      <button key={m.id || m.username} className="member" type="button" onClick={() => setMember(m)}>
                        <Avatar className="avatar" name={m.name || m.first_name || m.username} />
                        <span>{m.name || `${m.first_name || ""} ${m.last_name || ""}`.trim() || m.username}</span>
                      </button>
                    ))}
                  </>
                ) : (
                  <div className="media-list">
                    {!mediaItems.length ? <p className="empty">No {infoTab} yet.</p> : null}
                    {mediaItems.map((item) => (
                      <button key={item.id} className="media-hit" type="button" onClick={() => item.src && setViewer({ src: item.src, caption: item.caption })}>
                        {item.src ? <img src={item.src} alt="" /> : <span>{item.title || item.url || item.context || item.date}</span>}
                      </button>
                    ))}
                  </div>
                )}
              </>
            )}
          </div>
        </aside>
      </div>

      {emojiOpen ? (
        <div className="emoji-pop">
          <div className="emoji-tabs">
            {Object.keys(EMOJIS).map((tab) => (
              <button key={tab} type="button" className={emojiTab === tab ? "active" : ""} onClick={() => setEmojiTab(tab)}>{tab}</button>
            ))}
          </div>
          <div className="emoji-grid">
            {(EMOJIS[emojiTab] || []).map((e) => (
              <button key={e} type="button" onClick={() => { setDraft((d) => d + e); composerRef.current?.focus(); }}>{e}</button>
            ))}
          </div>
        </div>
      ) : null}

      <Modal open={modal === "contacts"} title="Contacts Book" onClose={() => setModal(null)}>
        <div className="search-mini"><input placeholder="Search by name or number..." value={contactQ} onChange={(e) => setContactQ(e.target.value)} /></div>
        {filteredContacts.map((c) => {
          const name = `${c.first_name || ""} ${c.last_name || ""}`.trim() || c.username || "Unknown";
          return (
            <button key={c.id} className="contact" type="button" onClick={() => { setModal(null); openChat({ id: c.id, title: name, type: "private" }); }}>
              <Avatar name={name} />
              <div><b>{name}</b><div className="muted">{c.username ? `@${c.username}` : c.phone ? `+${c.phone}` : ""}</div></div>
            </button>
          );
        })}
      </Modal>

      <Modal open={modal === "new"} title="New Message" onClose={() => setModal(null)}>
        <div className="field">
          <input
            placeholder="Search contacts, @username or t.me link..."
            value={contactQ}
            onChange={(e) => setContactQ(e.target.value)}
            onKeyDown={(e) => { if (e.key === "Enter") smart(contactQ.trim()); }}
          />
        </div>
        {filteredContacts.map((c) => {
          const name = `${c.first_name || ""} ${c.last_name || ""}`.trim() || c.username || "Unknown";
          return (
            <button key={c.id} className="contact" type="button" onClick={() => { setModal(null); openChat({ id: c.id, title: name, type: "private" }); }}>
              <Avatar name={name} /><span>{name}</span>
            </button>
          );
        })}
        <div className="row">
          <button className="btn ghost" type="button" onClick={() => contactQ && api.joinChat(phone, contactQ).then((r) => toast(r.status === "success" ? "Joined chat" : r.reason || "Join failed", r.status === "success" ? "success" : "error"))}>Join link</button>
          <button className="btn primary" type="button" onClick={() => smart(contactQ.trim())}>Open chat</button>
        </div>
      </Modal>

      <Modal open={modal === "search"} title="Search results" onClose={() => setModal(null)}>
        {!searchHits.length ? <p className="empty">No results</p> : null}
        {searchHits.map((hit) => (
          <button key={`${hit.type}-${hit.id}`} className="contact" type="button" onClick={() => { setModal(null); smart(hit.username ? `@${hit.username}` : hit.id); }}>
            <Avatar name={hit.title} />
            <div><b>{hit.title}</b><div className="muted">{hit.description || hit.type}</div></div>
          </button>
        ))}
      </Modal>

      <Modal open={modal === "folders"} title="Chat Folders" onClose={() => setModal(null)}>
        {FOLDERS.map((f) => (
          <button key={f.id} className="folder" type="button" onClick={() => { setFilter(f.filter === "private" ? "personal" : f.filter); setModal(null); }}>
            {f.name}
          </button>
        ))}
      </Modal>

      <Modal open={modal === "settings"} title="Settings" onClose={() => setModal(null)} wide>
        <div className="settings">
          <nav>
            {SETTINGS_NAV.map((p) => (
              <button key={p} type="button" className={settingsPage === p ? "active" : ""} onClick={() => setSettingsPage(p)}>{p[0].toUpperCase() + p.slice(1)}</button>
            ))}
          </nav>
          <article>
            {settingsPage === "general" && <p>Theme: sky blue and white. Sessions stay live until you switch accounts.</p>}
            {settingsPage === "notifications" && <p>Desktop toasts show send, delete, forward and session errors.</p>}
            {settingsPage === "privacy" && <p>API calls use the process token from /web-config.js. Logout clears the selected session only.</p>}
            {settingsPage === "appearance" && <p>The product workspace is sky blue cards on a white gradient.</p>}
            {settingsPage === "language" && <p>English</p>}
            {settingsPage === "accounts" && <p>{accounts.length} sessions loaded. Active: {activeCount}.</p>}
            {settingsPage === "templates" && <p>Open Message Templates from Quick Actions to edit saved replies.</p>}
            {settingsPage === "premium" && <p>Premium messaging features are enabled for this console.</p>}
            {settingsPage === "about" && <p>TG Gateway control center — live Telegram sessions.</p>}
          </article>
        </div>
      </Modal>

      <Modal open={modal === "templates"} title="Message Templates" onClose={() => setModal(null)}>
        <div className="field">
          <input value={templateDraft} onChange={(e) => setTemplateDraft(e.target.value)} placeholder="Save a reply..." />
        </div>
        <button className="btn primary" type="button" onClick={() => {
          if (!templateDraft.trim()) return;
          const next = [...templates, templateDraft.trim()];
          setTemplates(next);
          saveTemplates(next);
          setTemplateDraft("");
        }}>Save template</button>
        {templates.map((tpl, i) => (
          <button key={i} className="folder" type="button" onClick={() => { setModal(null); send(tpl); }}>
            {tpl}
          </button>
        ))}
      </Modal>

      <Modal open={modal === "analytics"} title="Session Node Analytics" onClose={() => setModal(null)}>
        <div className="stats">
          <div className="stat"><span>Total Sessions</span><b>{analytics.total || accounts.length}</b></div>
          <div className="stat"><span>Active</span><b>{analytics.active || 0}</b></div>
          <div className="stat"><span>Banned</span><b>{analytics.banned || 0}</b></div>
          <div className="stat"><span>Flooded</span><b>{analytics.flooded || 0}</b></div>
        </div>
        <p>Phone +{phone || "-"}</p>
        <p>Data Center {analytics.profile?.dc_id || "-"}</p>
        <p>Network {typeof analytics.profile?.proxy === "object" ? `${analytics.profile.proxy.host}:${analytics.profile.proxy.port}` : analytics.profile?.proxy || "Direct"}</p>
        <p>SpamBot {analytics.profile?.restricted || "Unknown"}</p>
        <p>{analytics.health?.details || "All systems operational"}</p>
      </Modal>

      <Modal open={modal === "mass"} title="Mass Operations" onClose={() => setModal(null)}>
        <div className="field">
          <label>Operation Type</label>
          <select value={massType} onChange={(e) => setMassType(e.target.value)}>
            {MASS_TYPES.map((t) => <option key={t.id} value={t.id}>{t.label}</option>)}
          </select>
        </div>
        <div className="field"><label>Target Channel/Group</label><input value={massTarget} onChange={(e) => setMassTarget(e.target.value)} placeholder="@username or https://t.me/..." /></div>
        <div className="field"><label>Account Limit</label><input type="number" value={massLimit} onChange={(e) => setMassLimit(e.target.value)} /></div>
        <button
          className="btn primary"
          type="button"
          disabled={busy}
          onClick={async () => {
            if (!massTarget.trim()) return toast("Enter a target", "warning");
            setBusy(true);
            const res = await api.massExecute(massTarget.trim());
            setBusy(false);
            toast(res.status === "success" ? `${MASS_TYPES.find((t) => t.id === massType)?.label || "Batch"} started` : res.reason || "Execution failed", res.status === "success" ? "success" : "error");
          }}
        >Execute Batch</button>
        <div className="logbox">{massLogs}</div>
      </Modal>

      <Modal open={modal === "forward"} title="Forward To..." onClose={() => setModal(null)}>
        <input placeholder="Search chats..." value={fwdQ} onChange={(e) => setFwdQ(e.target.value)} />
        {dialogs.filter((d) => d.title.toLowerCase().includes(fwdQ.toLowerCase())).map((d) => (
          <button
            key={d.id}
            className="fwd"
            type="button"
            onClick={async () => {
              const res = await api.forwardMessage(phone, chat.id, d.id, forwardId);
              setModal(null);
              toast(res.status === "success" ? "Message forwarded!" : res.reason || "Forward failed", res.status === "success" ? "success" : "error");
              if (res.status === "success" && chat?.id === d.id) reloadHistory(d.id);
            }}
          >{d.title}</button>
        ))}
      </Modal>

      <Modal open={modal === "delete"} title="Delete Message" onClose={() => setModal(null)}>
        <p>Are you sure? This action cannot be undone.</p>
        <label className="field"><input type="checkbox" checked={deleteEveryone} onChange={(e) => setDeleteEveryone(e.target.checked)} /> Also delete for everyone</label>
        <div className="row">
          <button className="btn ghost" type="button" onClick={() => setModal(null)}>Cancel</button>
          <button
            className="btn danger"
            type="button"
            onClick={async () => {
              const res = await api.deleteMessage(phone, chat.id, deleteTarget, deleteEveryone);
              setModal(null);
              if (res.status === "success") {
                setMessages((m) => m.filter((x) => String(x.id) !== String(deleteTarget)));
                setHistoryCache((c) => ({ ...c, [chat.id]: (c[chat.id] || []).filter((x) => String(x.id) !== String(deleteTarget)) }));
              }
              toast(res.status === "success" ? (deleteEveryone ? "Deleted for everyone" : "Deleted for you") : res.reason || "Delete failed", res.status === "success" ? "success" : "error");
            }}
          >Delete</button>
        </div>
      </Modal>

      <Modal open={!!call} title={call?.type === "video" ? "Video Call" : "Voice Call"} onClose={() => setCall(null)}>
        <div className="call">
          <Avatar className="avatar" name={call?.name} />
          <h3>{call?.name}</h3>
          <p>Calling...</p>
          <button className="btn danger" type="button" onClick={() => setCall(null)}>End</button>
        </div>
      </Modal>

      {viewer ? (
        <div className="lightbox" onClick={() => setViewer(null)}>
          <img src={viewer.src} alt="" />
          {viewer.caption ? <p>{viewer.caption}</p> : null}
        </div>
      ) : null}
    </div>
  );
}
