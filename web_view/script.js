// ===================================================================
// 🚀 TELEGRAM WEB K PREMIUM — Enterprise Live Database-Linked Engine
//     Complete A-to-Z Telegram UI Clone with 1000+ Account Support
//     Premium Edition — All Pages, Panels & Features
// ===================================================================

// ===================================================================
// 0. GLOBAL STATE MANAGEMENT
// ===================================================================
window.TG = window.TG || {};

const TG_STATE = {
    // Account Management
    selectedPhone: null,
    accounts: [],
    accountsMap: {},
    
    // Chat Management
    currentChatId: null,
    currentChatTitle: null,
    currentChatType: null, // 'private', 'group', 'channel', 'saved'
    dialogsCache: [],
    chatHistoryCache: {},
    
    // UI State
    theme: localStorage.getItem('tg_theme') || 'dark',
    activeFilter: 'all',
    searchQuery: '',
    replyTo: null,
    replyToMsg: null,
    editMessageId: null,
    forwardTarget: null,
    deleteTarget: null,
    
    // Message State
    msgCounter: Date.now(),
    pinnedMessages: [],
    scheduledMessages: [],
    draftMessages: {},
    
    // Analytics
    analyticsData: null,
    healthMetricsInterval: null,
    
    // Connection Pool
    connectionStatus: 'disconnected',
    connectionPool: {},
    
    // Premium
    isPremium: true,
    premiumFeatures: {
        animatedEmoji: true,
        stickers: true,
        gifs: true,
        voiceMessages: true,
        videoMessages: true,
        polls: true,
        quiz: true,
        topics: true,
        reactions: true,
        reply: true,
        forward: true,
        edit: true,
        delete: true,
        schedule: true,
        pin: true,
        archive: true,
        folders: true,
        filters: true,
        savedMessages: true,
        contactsSync: true,
        calls: true,
        stories: true,
        wallet: true,
        bots: true,
        channels: true,
        groups: true,
        supergroups: true,
        topics2: true
    }
};

// ===================================================================
// 0.5 DYNAMIC PREMIUM STYLES INJECTION
// ===================================================================
(function injectPremiumStyles() {
    const style = document.createElement('style');
    style.textContent = `
        @keyframes fadeInUp { from { opacity:0; transform:translateY(15px); } to { opacity:1; transform:translateY(0); } }
        @keyframes fadeIn { from { opacity:0; } to { opacity:1; } }
        @keyframes slideInRight { from { transform:translateX(100%); opacity:0; } to { transform:translateX(0); opacity:1; } }
        @keyframes slideInLeft { from { transform:translateX(-100%); opacity:0; } to { transform:translateX(0); opacity:1; } }
        @keyframes slideInUp { from { transform:translateY(100%); opacity:0; } to { transform:translateY(0); opacity:1; } }
        @keyframes slideInDown { from { transform:translateY(-100%); opacity:0; } to { transform:translateY(0); opacity:1; } }
        @keyframes pulse { 0%,100% { opacity:0.4; } 50% { opacity:1; } }
        @keyframes ripple { to { transform:scale(4); opacity:0; } }
        @keyframes shake { 0%,100% { transform:translateX(0); } 25% { transform:translateX(-5px); } 75% { transform:translateX(5px); } }
        @keyframes spin { from { transform:rotate(0deg); } to { transform:rotate(360deg); } }
        @keyframes typing { 0% { transform:translateY(0); } 50% { transform:translateY(-5px); } 100% { transform:translateY(0); } }
        @keyframes messageIn { from { opacity:0; transform:translateY(10px) scale(0.98); } to { opacity:1; transform:translateY(0) scale(1); } }
        @keyframes skeletonPulse { 0% { background-position: -200px 0; } 100% { background-position: calc(200px + 100%) 0; } }
        @keyframes notificationIn { from { opacity:0; transform:translateX(100%); } to { opacity:1; transform:translateX(0); } }
        
        .fade-in-up { animation: fadeInUp 0.3s ease forwards; opacity:0; }
        .fade-in { animation: fadeIn 0.3s ease forwards; }
        .slide-right { animation: slideInRight 0.3s cubic-bezier(0.4,0,0.2,1); }
        .slide-left { animation: slideInLeft 0.3s cubic-bezier(0.4,0,0.2,1); }
        .message-anim { animation: messageIn 0.2s ease forwards; }
        .typing-indicator span { animation: typing 1.4s infinite; display:inline-block; }
        .typing-indicator span:nth-child(2) { animation-delay:0.2s; }
        .typing-indicator span:nth-child(3) { animation-delay:0.4s; }
        
        .skeleton { background: linear-gradient(90deg, var(--bg-secondary) 25%, var(--bg-hover) 50%, var(--bg-secondary) 75%); background-size: 200px 100%; animation: skeletonPulse 1.5s ease-in-out infinite; border-radius: 4px; }
        
        .toast-container { position:fixed; top:16px; right:16px; z-index:10000; display:flex; flex-direction:column; gap:8px; }
        .toast-item { background:var(--bg-primary); color:var(--text-primary); padding:12px 20px; border-radius:12px; box-shadow:0 4px 20px rgba(0,0,0,0.3); animation:notificationIn 0.3s ease; border-left:3px solid var(--accent-blue); min-width:200px; max-width:400px; display:flex; align-items:center; gap:10px; font-size:14px; cursor:pointer; transition:all 0.2s; }
        .toast-item.success { border-left-color:var(--success); }
        .toast-item.error { border-left-color:var(--error); }
        .toast-item.warning { border-left-color:var(--warning); }
        .toast-item:hover { transform:translateX(-4px); }
    `;
    document.head.appendChild(style);
    
    // Create toast container
    const tc = document.createElement('div');
    tc.className = 'toast-container';
    tc.id = 'toastContainer';
    document.body.appendChild(tc);
})();

// ===================================================================
// 1. THEME ENGINE
// ===================================================================
function applyTheme(theme) {
    TG_STATE.theme = theme;
    document.documentElement.setAttribute('data-theme', theme);
    document.body.setAttribute('data-theme', theme);
    localStorage.setItem('tg_theme', theme);
    
    const icon = document.querySelector('#themeToggleBtn i');
    if (icon) icon.className = theme === 'dark' ? 'fas fa-sun' : 'fas fa-moon';
}

function toggleTheme() {
    applyTheme(TG_STATE.theme === 'dark' ? 'light' : 'dark');
}

// ===================================================================
// 2. TOAST NOTIFICATION SYSTEM
// ===================================================================
let toastId = 0;

function showToast(message, type = 'info', duration = 3000) {
    const container = document.getElementById('toastContainer');
    if (!container) return;
    
    const id = ++toastId;
    const toast = document.createElement('div');
    toast.className = `toast-item ${type}`;
    toast.id = `toast-${id}`;
    
    const icons = { info: 'fa-info-circle', success: 'fa-check-circle', error: 'fa-times-circle', warning: 'fa-exclamation-triangle' };
    const colors = { info: 'var(--accent-blue)', success: 'var(--success)', error: 'var(--error)', warning: 'var(--warning)' };
    
    toast.innerHTML = `<i class="fas ${icons[type] || icons.info}" style="color:${colors[type] || colors.info};"></i><span>${message}</span>`;
    container.appendChild(toast);
    
    setTimeout(() => {
        const el = document.getElementById(`toast-${id}`);
        if (el) {
            el.style.transition = 'opacity 0.3s ease, transform 0.3s ease';
            el.style.opacity = '0';
            el.style.transform = 'translateX(50px)';
            setTimeout(() => el.remove(), 300);
        }
    }, duration);
}

// ===================================================================
// 3. UTILITY FUNCTIONS
// ===================================================================
function escapeHtml(text) {
    if (!text) return '';
    const d = document.createElement('div');
    d.textContent = text;
    return d.innerHTML;
}

function linkifyText(html) {
    if (!html) return '';
    const urlPattern = /(\b(https?|ftp|file):\/\/[-A-Z0-9+&@#\/%?=~_|!:,.;]*[-A-Z0-9+&@#\/%=~_|])/gi;
    const tgPattern = /(?:@|t\.me\/)([a-zA-Z0-9_]{5,32})/g;
    
    let result = html.replace(urlPattern, (url) => {
        if (url.includes('t.me') || url.includes('telegram.me')) {
            return `<a href="javascript:void(0);" class="tg-link" onclick="event.stopPropagation(); TG.smartRoute('${url}')" style="color:var(--accent-blue);text-decoration:underline;font-weight:600;">${url}</a>`;
        }
        return `<a href="${url}" target="_blank" class="external-link" onclick="event.stopPropagation();" style="color:var(--accent-blue);text-decoration:underline;">${url}</a>`;
    });
    
    result = result.replace(tgPattern, (match, username) => {
        if (match.startsWith('@')) {
            return `<a href="javascript:void(0);" onclick="event.stopPropagation(); TG.smartRoute('${username}')" style="color:var(--accent-blue);font-weight:600;">${match}</a>`;
        }
        return match;
    });
    
    return result;
}

function formatTime(timestamp) {
    if (!timestamp) return '';
    const d = new Date(timestamp * 1000);
    const now = new Date();
    const diff = now - d;
    const days = Math.floor(diff / (1000 * 60 * 60 * 24));
    
    if (days === 0) return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    if (days === 1) return 'Yesterday';
    if (days < 7) return d.toLocaleDateString([], { weekday: 'short' });
    return d.toLocaleDateString([], { day: 'numeric', month: 'short' });
}

function formatDateFull(timestamp) {
    if (!timestamp) return '';
    return new Date(timestamp * 1000).toLocaleDateString([], { 
        weekday: 'long', year: 'numeric', month: 'long', day: 'numeric',
        hour: '2-digit', minute: '2-digit'
    });
}

function getInitials(name) {
    if (!name) return '?';
    return name.charAt(0).toUpperCase();
}

function generateMessageId() {
    return `msg_${TG_STATE.msgCounter++}_${Math.random().toString(36).substr(2, 6)}`;
}

function debounce(fn, delay = 300) {
    let timer;
    return function(...args) {
        clearTimeout(timer);
        timer = setTimeout(() => fn.apply(this, args), delay);
    };
}

function throttle(fn, limit = 300) {
    let inThrottle = false;
    return function(...args) {
        if (!inThrottle) {
            fn.apply(this, args);
            inThrottle = true;
            setTimeout(() => inThrottle = false, limit);
        }
    };
}

function normalizeDialogType(type) {
    const map = {
        people: 'private', private: 'private', personal: 'private',
        groups: 'group', group: 'group', supergroup: 'group',
        channels: 'channel', channel: 'channel',
        bots: 'bot', bot: 'bot', saved: 'saved', service: 'service'
    };
    return map[type] || type || 'private';
}

function dialogMatchesFilter(dialog, filter) {
    const type = normalizeDialogType(dialog.type);
    if (filter === 'all') return true;
    if (filter === 'personal' || filter === 'private') return type === 'private';
    if (filter === 'groups' || filter === 'group') return type === 'group';
    if (filter === 'channels' || filter === 'channel') return type === 'channel';
    if (filter === 'bots' || filter === 'bot') return type === 'bot';
    return true;
}

function normalizeMessage(msg) {
    return {
        id: msg.id,
        text: msg.text || '',
        date: msg.date || Math.floor(Date.now() / 1000),
        outgoing: msg.outgoing ?? msg.is_self ?? false,
        status: msg.status || ((msg.outgoing ?? msg.is_self) ? 'sent' : 'read'),
        reply_to_msg_id: msg.reply_to_msg_id || null,
        reply_to_sender: msg.reply_to_sender || msg.sender_name || '',
        reply_to_text: msg.reply_to_text || '',
        forward_from: msg.forward_from || null,
        media: msg.media || null,
        edited: msg.edited || false,
        reactions: msg.reactions || []
    };
}

// #region agent log
function debugCompatLog(location, message, data, hypothesisId) {
    fetch('http://127.0.0.1:7667/ingest/ad17e3a3-7e7d-4c52-8b31-97d632565ea9',{method:'POST',headers:{'Content-Type':'application/json','X-Debug-Session-Id':'a49f69'},body:JSON.stringify({sessionId:'a49f69',location,message,data,timestamp:Date.now(),hypothesisId})}).catch(()=>{});
}
// #endregion

// ===================================================================
// 4. API LAYER — All Backend Communication
// ===================================================================
const TG_API = {
    baseURL: '',
    
    async request(endpoint, options = {}) {
        const url = `${this.baseURL}${endpoint}`;
        const config = {
            headers: { 'Content-Type': 'application/json', ...options.headers },
            ...options
        };
        
        try {
            const res = await fetch(url, config);
            return await res.json();
        } catch (err) {
            console.error(`API Error [${endpoint}]:`, err);
            return { status: 'error', reason: 'Network Error' };
        }
    },
    
    get(endpoint) { return this.request(endpoint, { method: 'GET' }); },
    post(endpoint, data) { return this.request(endpoint, { method: 'POST', body: JSON.stringify(data) }); },
    put(endpoint, data) { return this.request(endpoint, { method: 'PUT', body: JSON.stringify(data) }); },
    delete(endpoint) { return this.request(endpoint, { method: 'DELETE' }); },
    
    // --- Account APIs ---
    getAccounts() { return this.get('/api/console/accounts'); },
    getProfile(phone) { return this.get(`/api/console/profile/${phone}`); },
    getContacts(phone) { return this.get(`/api/console/contacts/${phone}`); },
    
    // --- Chat APIs ---
    getDialogs(phone) { return this.get(`/api/console/dialogs/${phone}`); },
    getChatHistory(phone, chatId) { return this.get(`/api/console/chat-history/${phone}/${chatId}`); },
    getChatInfo(phone, chatId) { return this.get(`/api/console/chat-info/${phone}/${chatId}`); },
    getChatMembers(phone, chatId) { return this.get(`/api/console/chat-members/${phone}/${chatId}`); },
    
    // --- Message APIs ---
    sendMessage(phone, chatId, text, replyTo = null, editId = null) {
        return this.post('/api/console/send', { phone, chat_id: chatId, message: text, text, reply_to: replyTo, edit_id: editId });
    },
    deleteMessage(phone, chatId, msgId, forEveryone = false) {
        return this.post('/api/console/delete-message', { phone, chat_id: chatId, msg_id: parseInt(msgId, 10), delete_for_everyone: forEveryone });
    },
    forwardMessage(phone, fromChatId, toChatId, msgId) {
        return this.post('/api/console/forward', { phone, from_chat_id: fromChatId, to_chat_id: toChatId, msg_id: parseInt(msgId, 10) });
    },
    
    // --- Smart Route ---
    smartRoute(phone, target) {
        return this.post('/api/console/smart-route', { phone, target });
    },
    globalSearch(phone, query) {
        return this.get(`/api/console/global-search/${phone}?q=${encodeURIComponent(query)}`);
    },
    
    // --- Automation Logs ---
    getAutomationLogs() { return this.get('/api/console/automation-logs'); },
    massExecute(target) { return this.post('/api/console/mass-execute', { target_channel: target }); },
    
    // --- Analytics ---
    getAnalytics(phone) { return this.get(`/api/console/analytics/${phone}`); },
    getSessionHealth(phone) { return this.get(`/api/console/health/${phone}`); }
};

// ===================================================================
// 5. ACCOUNT MANAGEMENT SYSTEM
// ===================================================================
const AccountManager = {
    accounts: [],
    accountsMap: {},
    
    async loadAccounts() {
        try {
            const res = await TG_API.getAccounts();
            if (res && Array.isArray(res)) {
                this.accounts = res;
                this.accountsMap = {};
                res.forEach(acc => { this.accountsMap[acc.phone] = acc; });
                return res;
            }
            return [];
        } catch (err) {
            console.error('Account load error:', err);
            return [];
        }
    },
    
    getAccount(phone) { return this.accountsMap[phone] || null; },
    getActiveAccounts() { return this.accounts.filter(a => a.status === 'active'); },
    getTotalCount() { return this.accounts.length; },
    
    selectAccount(phone) {
        TG_STATE.selectedPhone = phone;
        TG_STATE.connectionStatus = 'connecting';
        
        // Trigger UI updates
        this.updateProfileUI(phone);
        this.loadDialogs(phone);
        
        // Store in session
        sessionStorage.setItem('tg_selected_phone', phone);
        
        // Trigger analytics refresh
        this.refreshConnectionStatus();
    },
    
    async updateProfileUI(phone) {
        const avatarEl = document.querySelector('.user-avatar');
        const nameEl = document.querySelector('.user-info h3');
        const statusEl = document.querySelector('.user-info p');
        
        if (!avatarEl || !nameEl || !statusEl) return;
        
        // Setup initial loading state
        avatarEl.innerHTML = '<i class="fas fa-spinner fa-spin" style="font-size:20px;color:white;"></i>';
        avatarEl.style.background = 'var(--accent-blue)';
        nameEl.textContent = 'Connecting...';
        statusEl.textContent = `+${phone}`;
        
        const res = await TG_API.getProfile(phone);
        if (res.status === 'success') {
            // 🔥 BUG FIXED: Read keys directly from root response object instead of res.profile
            if (res.profile_pic) {
                avatarEl.innerHTML = `<img src="${res.profile_pic}" style="width:100%;height:100%;border-radius:50%;object-fit:cover;">`;
                avatarEl.style.background = 'none';
            } else {
                avatarEl.innerHTML = getInitials(res.full_name || phone);
                avatarEl.style.background = 'linear-gradient(135deg, var(--accent-blue), var(--secondary-blue))';
            }
            nameEl.textContent = res.full_name || 'User';
            statusEl.innerHTML = `+${res.phone}${res.username ? ` <span style="color:var(--accent-blue);font-size:11px;">@${res.username}</span>` : ''}`;
            TG_STATE.connectionStatus = 'connected';
        } else {
            avatarEl.innerHTML = getInitials(phone);
            avatarEl.style.background = 'linear-gradient(135deg, var(--accent-blue), var(--secondary-blue))';
            nameEl.textContent = 'Session ' + phone.slice(-4);
            statusEl.textContent = `+${phone}`;
            TG_STATE.connectionStatus = 'connected';
        }
        this.refreshConnectionStatus(); // Update connection status dot instantly
    },
    
    refreshConnectionStatus() {
        const indicator = document.querySelector('.connection-status');
        if (indicator) {
            const statusMap = {
                'disconnected': { color: 'var(--error)', text: 'Disconnected' },
                'connecting': { color: 'var(--warning)', text: 'Connecting...' },
                'connected': { color: 'var(--success)', text: 'Online' }
            };
            const s = statusMap[TG_STATE.connectionStatus] || statusMap.disconnected;
            indicator.style.color = s.color;
            indicator.title = s.text;
        }
    },
    
    async loadDialogs(phone) {
        const chatList = document.getElementById('chatList');
        if (!chatList) return;
        
        const res = await TG_API.getDialogs(phone);
        if (res.status === 'success' && res.dialogs) {
            TG_STATE.dialogsCache = res.dialogs;
            this.renderDialogs(res.dialogs);
        } else {
            chatList.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-secondary);">No conversations found.<br><small>Start a new chat or sync your account.</small></div>';
        }
    },
    
    renderDialogs(dialogs, filter = 'all') {
        const chatList = document.getElementById('chatList');
        if (!chatList) return;
        
        let filtered = dialogs;
        if (filter !== 'all') {
            filtered = dialogs.filter(d => dialogMatchesFilter(d, filter));
        }
        
        // Apply search filter
        if (TG_STATE.searchQuery) {
            const q = TG_STATE.searchQuery.toLowerCase();
            filtered = filtered.filter(d => 
                d.title.toLowerCase().includes(q) || 
                (d.last_message && d.last_message.toLowerCase().includes(q))
            );
        }
        
        if (filtered.length === 0) {
            chatList.innerHTML = '<div style="padding:30px;text-align:center;color:var(--text-secondary);"><i class="fas fa-inbox" style="font-size:32px;display:block;margin-bottom:10px;opacity:0.3;"></i>No chats found</div>';
            return;
        }
        
        chatList.innerHTML = '';
        filtered.forEach((chat, index) => {
            const div = document.createElement('div');
            div.className = `chat-item fade-in-up ${TG_STATE.currentChatId === chat.id ? 'active' : ''}`;
            div.style.animationDelay = `${(index % 20) * 0.03}s`;
            div.dataset.chatId = chat.id;
            div.dataset.chatType = chat.type;
            
            const isUnread = chat.unread_count > 0;
            const isMuted = chat.muted;
            const isPinned = chat.pinned;
            const initials = getInitials(chat.title);
            const avatarBg = chat.type === 'group' ? 'linear-gradient(135deg, #6C5CE7, #a29bfe)' : 
                             chat.type === 'channel' ? 'linear-gradient(135deg, #00b894, #55efc4)' :
                             'linear-gradient(135deg, var(--accent-blue), var(--secondary-blue))';
            
            let lastMsgHtml = chat.last_message ? escapeHtml(chat.last_message.substring(0, 80)) : '';
            if (lastMsgHtml && lastMsgHtml.length >= 80) lastMsgHtml += '...';
            
            div.innerHTML = `
                <div class="chat-avatar" style="background:${avatarBg};">${chat.photo ? `<img src="${chat.photo}" style="width:100%;height:100%;border-radius:50%;object-fit:cover;">` : initials}</div>
                <div class="chat-content">
                    <div class="chat-header">
                        <span class="chat-name">${escapeHtml(chat.title)}</span>
                        <span class="chat-time">${formatTime(chat.last_date)}</span>
                        ${isPinned ? '<i class="fas fa-thumbtack" style="font-size:10px;color:var(--text-secondary);margin-left:4px;"></i>' : ''}
                    </div>
                    <div class="chat-message">
                        ${isMuted ? '<i class="fas fa-bell-slash" style="font-size:10px;color:var(--text-secondary);margin-right:4px;"></i>' : ''}
                        ${chat.typing ? '<span style="color:var(--accent-blue);">typing...</span>' : (lastMsgHtml || 'No messages')}
                        ${isUnread ? `<span class="unread-badge">${chat.unread_count > 99 ? '99+' : chat.unread_count}</span>` : ''}
                    </div>
                </div>
            `;
            
            div.onclick = () => this.openChat(chat.id, chat.title, chat.type);
            chatList.appendChild(div);
        });
    },
    
    async openChat(chatId, title, type) {
        if (!TG_STATE.selectedPhone) {
            showToast('Please select a session first', 'warning');
            return;
        }
        
        TG_STATE.currentChatId = chatId;
        TG_STATE.currentChatTitle = title;
        TG_STATE.currentChatType = type || 'private';
        
        // 🔥 FIX 1: Toggle Chat Window Visibility (Placeholder vs Actual Content)
        const placeholder = document.getElementById('chatWindowPlaceholder');
        const actualContent = document.getElementById('chatWindowActualContent');
        if (placeholder) placeholder.style.display = 'none';
        if (actualContent) actualContent.style.display = 'flex';

        // 🔥 FIX 2: Mobile/Responsive Pane Handling
        if (window.innerWidth <= 768) {
            document.body.classList.add('chat-active');
            document.getElementById('chatListPanel')?.classList.add('hide');
        }
        
        // Update header elements
        const headerName = document.getElementById('headerName');
        const headerStatus = document.getElementById('headerStatus');
        const headerAvatar = document.getElementById('headerAvatar');
        if (headerName) headerName.textContent = title;
        if (headerStatus) headerStatus.textContent = type === 'group' ? 'Group' : type === 'channel' ? 'Channel' : 'online';
        if (headerAvatar) headerAvatar.textContent = getInitials(title);
        
        // Update active state in dialog list
        document.querySelectorAll('.chat-item').forEach(el => {
            el.classList.toggle('active', el.dataset.chatId === chatId);
        });
        
        // Load chat history
        await this.loadChatHistory(chatId, title);
        
        // Enable input
        const msgInput = document.getElementById('messageInput');
        if (msgInput) {
            msgInput.disabled = false;
            msgInput.placeholder = `Message ${title}`;
            msgInput.focus();
        }
        
        // Load chat info for right panel
        this.loadChatInfo(chatId);
    },
    
    async loadChatHistory(chatId, title) {
        // 🔥 FIX 3: Changed 'chatMessages' to 'messages' matching index_2.html
        const messagesContainer = document.getElementById('messages');
        if (!messagesContainer) return;
        
        messagesContainer.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text-secondary);"><i class="fas fa-spinner fa-spin" style="font-size:24px;display:block;margin-bottom:10px;"></i>Loading messages...</div>';
        
        if (TG_STATE.chatHistoryCache[chatId]) {
            this.renderMessages(TG_STATE.chatHistoryCache[chatId], title);
            return;
        }
        
        const res = await TG_API.getChatHistory(TG_STATE.selectedPhone, chatId);
        // #region agent log
        debugCompatLog('script.js:loadChatHistory', 'Chat history API response', { status: res?.status, count: res?.messages?.length || 0, chatId }, 'H2');
        // #endregion
        if (res.status === 'success' && res.messages) {
            TG_STATE.chatHistoryCache[chatId] = res.messages;
            this.renderMessages(res.messages, title);
        } else {
            messagesContainer.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text-secondary);">No messages yet.<br><small>Be the first to send a message!</small></div>';
        }
    },
    
    renderMessages(messages, title) {
        const container = document.getElementById('messages');
        if (!container) return;
        
        if (!messages || messages.length === 0) {
            container.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text-secondary);">No messages yet.</div>';
            return;
        }
        
        container.innerHTML = '';
        
        let lastDate = null;
        messages.map(normalizeMessage).forEach((msg, index) => {
            const msgDate = new Date(msg.date * 1000).toDateString();
            
            // Date separator
            if (msgDate !== lastDate) {
                lastDate = msgDate;
                const dateDiv = document.createElement('div');
                dateDiv.className = 'date-separator';
                dateDiv.textContent = formatDateFull(msg.date);
                container.appendChild(dateDiv);
            }
            
            const isOutgoing = msg.outgoing;
            const div = document.createElement('div');
            div.className = `message ${isOutgoing ? 'sent' : 'received'} message-anim`;
            div.dataset.msgId = msg.id;
            
            // Build message content
            let content = '';
            
            // Reply preview
            if (msg.reply_to_msg_id) {
                content += `<div class="reply-preview"><div class="reply-line"></div><div class="reply-content"><strong>${msg.reply_to_sender || 'Reply'}</strong><span>${escapeHtml((msg.reply_to_text || '').substring(0, 50))}</span></div></div>`;
            }
            
            // Forward header
            if (msg.forward_from) {
                content += `<div class="forward-header"><i class="fas fa-share"></i> Forwarded from <strong>${escapeHtml(msg.forward_from)}</strong></div>`;
            }
            
            // Message text
            if (msg.text) {
                content += `<div class="message-content">${linkifyText(escapeHtml(msg.text))}</div>`;
            }
            
            // Inside the message rendering loop
            if (msg.media) {
                // 🔥 FIX: Safe fallback for missing media.url
                if (msg.media.type === 'photo') {
                    if (msg.media.url) {
                        content += `<div class="media-container"><img src="${msg.media.url}" alt="Photo" class="media-image" onclick="window.open('${msg.media.url}','_blank')"></div>`;
                    } else {
                        content += `<div class="media-container" style="padding:20px;text-align:center;background:var(--bg-secondary);border-radius:12px;"><i class="fas fa-image" style="font-size:32px;color:var(--text-secondary);"></i><br><small style="color:var(--text-secondary);">Photo</small></div>`;
                    }
                } else if (msg.media.type === 'video') {
                    if (msg.media.url) {
                        content += `<div class="media-container"><video src="${msg.media.url}" controls class="media-video" poster="${msg.media.thumb || ''}"></video></div>`;
                    } else {
                        content += `<div class="media-container" style="padding:20px;text-align:center;background:var(--bg-secondary);border-radius:12px;"><i class="fas fa-video" style="font-size:32px;color:var(--text-secondary);"></i><br><small style="color:var(--text-secondary);">Video</small></div>`;
                    }
                } else if (msg.media.type === 'audio') {
                    if (msg.media.url) {
                        content += `<div class="media-container audio-container"><i class="fas fa-music"></i><audio src="${msg.media.url}" controls style="width:100%;"></audio></div>`;
                    } else {
                        content += `<div class="media-container" style="padding:20px;text-align:center;background:var(--bg-secondary);border-radius:12px;"><i class="fas fa-music" style="font-size:32px;color:var(--text-secondary);"></i><br><small style="color:var(--text-secondary);">${escapeHtml(msg.media.filename || 'Audio')}</small></div>`;
                    }
                } else if (msg.media.type === 'file') {
                    if (msg.media.url) {
                        content += `<div class="media-container file-container"><i class="fas fa-file"></i><div><strong>${escapeHtml(msg.media.filename || 'File')}</strong><span>${msg.media.size || ''}</span></div><button class="download-btn" onclick="window.open('${msg.media.url}','_blank')"><i class="fas fa-download"></i></button></div>`;
                    } else {
                        content += `<div class="media-container" style="padding:20px;text-align:center;background:var(--bg-secondary);border-radius:12px;"><i class="fas fa-file" style="font-size:32px;color:var(--text-secondary);"></i><br><strong style="color:var(--text-primary);">${escapeHtml(msg.media.filename || 'File')}</strong><br><small style="color:var(--text-secondary);">${msg.media.size || ''}</small></div>`;
                    }
                } else if (msg.media.type === 'sticker') {
                    if (msg.media.url) {
                        content += `<div class="sticker-container"><img src="${msg.media.url}" alt="Sticker" class="sticker-image"></div>`;
                    } else {
                        content += `<div class="media-container" style="padding:20px;text-align:center;background:var(--bg-secondary);border-radius:12px;"><i class="fas fa-sticky-note" style="font-size:32px;color:var(--text-secondary);"></i><br><small style="color:var(--text-secondary);">Sticker</small></div>`;
                    }
                } else if (msg.media.type === 'gif') {
                    if (msg.media.url) {
                        content += `<div class="media-container"><img src="${msg.media.url}" alt="GIF" class="media-gif"></div>`;
                    } else {
                        content += `<div class="media-container" style="padding:20px;text-align:center;background:var(--bg-secondary);border-radius:12px;"><i class="fas fa-play-circle" style="font-size:32px;color:var(--text-secondary);"></i><br><small style="color:var(--text-secondary);">GIF</small></div>`;
                    }
                } else {
                    // fallback for unknown media types
                    content += `<div class="media-container" style="padding:20px;text-align:center;background:var(--bg-secondary);border-radius:12px;"><i class="fas fa-file-alt" style="font-size:32px;color:var(--text-secondary);"></i><br><small style="color:var(--text-secondary);">${escapeHtml(msg.media.type || 'Media')}</small></div>`;
                }
            }
            
            // Poll
            if (msg.poll) {
                content += this.renderPoll(msg.poll);
            }
            
            // Status + Time
            const statusIcons = { 
                'sent': 'fa-check', 
                'delivered': 'fa-check-double', 
                'read': 'fa-check-double',
                'failed': 'fa-exclamation-circle'
            };
            const statusColor = {
                'sent': 'var(--text-secondary)',
                'delivered': 'var(--text-secondary)',
                'read': 'var(--accent-blue)',
                'failed': 'var(--error)'
            };
            const status = msg.status || (isOutgoing ? 'sent' : 'read');
            
            content += `<div class="message-meta">
                <span class="message-time">${formatTime(msg.date)}</span>
                ${isOutgoing ? `<i class="fas ${statusIcons[status] || 'fa-check'}" style="color:${statusColor[status] || 'var(--text-secondary)'};font-size:11px;margin-left:4px;"></i>` : ''}
                ${msg.edited ? '<span style="font-size:10px;color:var(--text-secondary);margin-left:4px;">edited</span>' : ''}
            </div>`;
            
            // Reactions
            if (msg.reactions && msg.reactions.length > 0) {
                content += `<div class="reactions">${msg.reactions.map(r => `<span class="reaction ${r.mine ? 'mine' : ''}">${r.emoji} ${r.count > 1 ? r.count : ''}</span>`).join('')}</div>`;
            }
            
            div.innerHTML = content;
            
            // Hover actions
            if (isOutgoing || true) {
                const actionsDiv = document.createElement('div');
                actionsDiv.className = 'message-actions';
                actionsDiv.innerHTML = `
                    <button onclick="event.stopPropagation(); TG.replyToMessage('${msg.id}','${escapeHtml(msg.text || '').replace(/'/g, "\\'")}')" title="Reply"><i class="fas fa-reply"></i></button>
                    <button onclick="event.stopPropagation(); TG.forwardMessage('${msg.id}')" title="Forward"><i class="fas fa-share"></i></button>
                    ${isOutgoing ? `<button onclick="event.stopPropagation(); TG.editMessage('${msg.id}','${escapeHtml(msg.text || '').replace(/'/g, "\\'")}')" title="Edit"><i class="fas fa-edit"></i></button>` : ''}
                    <button onclick="event.stopPropagation(); TG.deleteMessage('${msg.id}')" title="Delete"><i class="fas fa-trash"></i></button>
                    <button onclick="event.stopPropagation(); TG.pinMessage('${msg.id}')" title="Pin"><i class="fas fa-thumbtack"></i></button>
                `;
                div.appendChild(actionsDiv);
            }
            
            container.appendChild(div);
        });
        
        // Scroll to bottom
        setTimeout(() => {
            container.scrollTop = container.scrollHeight;
        }, 100);
    },
    
    renderPoll(poll) {
        const total = poll.votes || 0;
        return `
            <div class="poll-container">
                <div class="poll-question">${escapeHtml(poll.question)}</div>
                ${poll.options.map(opt => {
                    const pct = total > 0 ? Math.round((opt.votes / total) * 100) : 0;
                    return `<div class="poll-option ${opt.selected ? 'selected' : ''}">
                        <div class="poll-bar" style="width:${pct}%;"></div>
                        <span class="poll-text">${escapeHtml(opt.text)}</span>
                        <span class="poll-pct">${pct}%</span>
                    </div>`;
                }).join('')}
                <div class="poll-total">${total} vote${total !== 1 ? 's' : ''}</div>
            </div>
        `;
    },
    
    async loadChatInfo(chatId) {
        if (!TG_STATE.selectedPhone) return;
        
        const panel = document.querySelector('.info-panel');
        if (!panel) return;
        
        const res = await TG_API.getChatInfo(TG_STATE.selectedPhone, chatId);
        if (res.status === 'success') {
            const info = res.info || res;
            
            const pName = document.getElementById('infoName');
            const pStatus = document.getElementById('infoStatus');
            const pAbout = document.getElementById('infoPanelTitle');
            const pPhoto = document.getElementById('infoAvatar');
            
            if (pName) pName.textContent = info.title || res.title || 'Chat';
            if (pStatus) pStatus.textContent = info.type || res.type || 'chat';
            if (pAbout) pAbout.textContent = (info.type === 'group' || res.type === 'group') ? 'Group Information' : 'Contact Information';
            if (pPhoto) {
                const photo = info.photo || res.photo;
                if (photo) {
                    pPhoto.innerHTML = `<img src="${photo}" style="width:100%;height:100%;border-radius:50%;object-fit:cover;">`;
                } else {
                    pPhoto.textContent = getInitials(info.title || res.title || '?');
                }
            }

            const memberCountEl = document.getElementById('infoMemberCount');
            if (memberCountEl) memberCountEl.textContent = info.member_count || res.member_count || 0;
            
            const chatType = info.type || res.type;
            if (chatType === 'group' || chatType === 'supergroup') {
                const members = res.members;
                if (members && members.length) {
                    this.renderMembers(members);
                } else {
                    TG_API.getChatMembers(TG_STATE.selectedPhone, chatId).then(mRes => {
                        if (mRes.status === 'success' && mRes.members) {
                            this.renderMembers(mRes.members);
                        }
                    });
                }
            }
        }
    },
    
    renderMembers(members) {
        const container = document.getElementById('infoMembersList');
        if (!container) return;
        
        container.innerHTML = '';
        members.forEach((member, index) => {
            const div = document.createElement('div');
            div.className = 'member-item fade-in-up';
            div.style.animationDelay = `${(index % 15) * 0.03}s`;
            const displayName = member.first_name || member.name || member.username || 'User';
            div.innerHTML = `
                <div class="member-avatar" style="background:linear-gradient(135deg,var(--accent-blue),var(--secondary-blue));">${getInitials(displayName)}</div>
                <div class="member-info">
                    <span class="member-name">${escapeHtml(displayName)} ${escapeHtml(member.last_name || '')}</span>
                    <span class="member-role">${member.role || 'member'}</span>
                </div>
            `;
            div.onclick = () => {
                TG.openMemberProfile(member);
            };
            container.appendChild(div);
        });
    },
    
    async sendTextMessage() {
        const input = document.getElementById('messageInput');
        if (!input) return;
        
        const text = input.value.trim();
        if (!text || !TG_STATE.currentChatId || !TG_STATE.selectedPhone) return;
        
        const tempId = generateMessageId();
        const tempMsg = {
            id: tempId, text: text, date: Math.floor(Date.now() / 1000),
            outgoing: true, status: 'sent', reply_to_msg_id: TG_STATE.replyTo
        };
        
        if (!TG_STATE.chatHistoryCache[TG_STATE.currentChatId]) {
            TG_STATE.chatHistoryCache[TG_STATE.currentChatId] = [];
        }
        TG_STATE.chatHistoryCache[TG_STATE.currentChatId].push(tempMsg);
        
        this.renderMessages(TG_STATE.chatHistoryCache[TG_STATE.currentChatId], TG_STATE.currentChatTitle);
        
        input.value = '';
        this.toggleSendButton();
        if (TG_STATE.replyTo) this.clearReply();
        
        const res = await TG_API.sendMessage(TG_STATE.selectedPhone, TG_STATE.currentChatId, text, TG_STATE.replyTo, TG_STATE.editMessageId);
        // #region agent log
        debugCompatLog('script.js:sendTextMessage', 'Send message API response', { status: res?.status, reason: res?.reason }, 'H3');
        // #endregion
        
        if (res.status !== 'success') {
            showToast('Failed to send message', 'error');
        } else {
            TG_STATE.editMessageId = null;
            setTimeout(() => this.loadChatHistory(TG_STATE.currentChatId, TG_STATE.currentChatTitle), 500);
        }
    },
    
    toggleSendButton() {
        const input = document.getElementById('messageInput');
        // 🔥 FIX: Changed 'sendMessageBtn' to 'sendBtn'
        const sendBtn = document.getElementById('sendBtn');
        if (!input || !sendBtn) return;
        
        if (input.value.trim().length > 0) {
            sendBtn.innerHTML = TG_STATE.editMessageId ? '<i class="fas fa-check"></i>' : '<i class="fas fa-paper-plane"></i>';
            sendBtn.disabled = false;
        } else {
            // Revert back to microphone icon if empty
            sendBtn.innerHTML = '<i class="fas fa-paper-plane"></i>';
        }
    },
    
    // --- Reply System ---
    replyToMessage(msgId, msgText) {
        TG_STATE.replyTo = msgId;
        const bar = document.getElementById('replyPreview');
        const textEl = document.getElementById('replyPreviewText');
        const nameEl = document.getElementById('replyPreviewName');
        if (bar) bar.style.display = 'block';
        if (textEl) textEl.textContent = msgText.substring(0, 80) + (msgText.length > 80 ? '...' : '');
        if (nameEl) nameEl.textContent = 'Replying';
        document.getElementById('messageInput')?.focus();
    },
    
    clearReply() {
        TG_STATE.replyTo = null;
        const bar = document.getElementById('replyPreview');
        if (bar) bar.style.display = 'none';
    },
    
    // --- Edit System ---
    editMessage(msgId, text) {
        TG_STATE.editMessageId = msgId;
        const input = document.getElementById('messageInput');
        if (input) {
            input.value = text;
            input.focus();
            this.toggleSendButton();
        }
        showToast('Editing message', 'info');
    },
    
    // --- Forward System ---
    forwardMessage(msgId) {
        TG_STATE.forwardTarget = msgId;
        const modal = document.getElementById('forwardChatPickerModal');
        if (modal) modal.classList.add('show');
        this.renderForwardChatList();
    },
    
    renderForwardChatList(search = '') {
        const container = document.getElementById('forwardChatsContainer');
        if (!container) return;
        
        let chats = TG_STATE.dialogsCache;
        if (search) {
            const q = search.toLowerCase();
            chats = chats.filter(c => c.title.toLowerCase().includes(q));
        }
        
        container.innerHTML = '';
        chats.forEach(chat => {
            const div = document.createElement('div');
            div.className = 'forward-chat-item';
            const initials = getInitials(chat.title);
            div.innerHTML = `
                <div style="width:36px;height:36px;border-radius:50%;background:var(--accent-blue);color:white;display:flex;align-items:center;justify-content:center;font-weight:600;flex-shrink:0;">${initials}</div>
                <span>${escapeHtml(chat.title)}</span>
            `;
            div.onclick = async () => {
                const modal = document.getElementById('forwardChatPickerModal');
                if (modal) modal.classList.remove('show');
                
                showToast(`Forwarding to ${chat.title}...`);
                const res = await TG_API.forwardMessage(TG_STATE.selectedPhone, TG_STATE.currentChatId, chat.id, TG_STATE.forwardTarget);
                if (res.status === 'success') {
                    showToast('Message forwarded!', 'success');
                    if (TG_STATE.currentChatId === chat.id) {
                        this.loadChatHistory(chat.id, chat.title);
                    }
                } else {
                    showToast('Forward failed: ' + (res.reason || 'Error'), 'error');
                }
                TG_STATE.forwardTarget = null;
            };
            container.appendChild(div);
        });
    },
    
    // --- Delete System ---
    deleteMessage(msgId) {
        TG_STATE.deleteTarget = msgId;
        const modal = document.getElementById('messageDeletionModal');
        if (modal) modal.classList.add('show');
    },
    
    async confirmDelete(forEveryone = false) {
        if (!TG_STATE.deleteTarget || !TG_STATE.currentChatId) return;
        
        const modal = document.getElementById('messageDeletionModal');
        if (modal) modal.classList.remove('show');
        
        // Optimistic UI removal
        const msgEl = document.querySelector(`.message[data-msg-id="${TG_STATE.deleteTarget}"]`);
        if (msgEl) {
            msgEl.style.transition = 'opacity 0.3s, transform 0.3s';
            msgEl.style.opacity = '0';
            msgEl.style.transform = 'scale(0.8)';
            setTimeout(() => msgEl.remove(), 300);
        }
        
        const res = await TG_API.deleteMessage(TG_STATE.selectedPhone, TG_STATE.currentChatId, TG_STATE.deleteTarget, forEveryone);
        if (res.status === 'success') {
            showToast(forEveryone ? 'Deleted for everyone' : 'Deleted for you', 'success');
        } else {
            showToast('Delete failed: ' + (res.reason || 'Error'), 'error');
            this.loadChatHistory(TG_STATE.currentChatId, TG_STATE.currentChatTitle);
        }
        TG_STATE.deleteTarget = null;
    },
    
    // --- Pin System ---
    async pinMessage(msgId) {
        showToast('Message pinned!', 'success');
        // Backend call would go here
    },
    
    // --- Smart Route ---
    async smartRoute(target) {
        if (!TG_STATE.selectedPhone) {
            showToast('Select a session first', 'warning');
            return;
        }
        
        const res = await TG_API.smartRoute(TG_STATE.selectedPhone, target);
        if (res.status === 'success') {
            TG_STATE.currentChatId = res.chat_id;
            TG_STATE.currentChatTitle = res.title || target;
            TG_STATE.currentChatType = 'private';
            
            const placeholder = document.getElementById('chatWindowPlaceholder');
            const actualContent = document.getElementById('chatWindowActualContent');
            if (placeholder) placeholder.style.display = 'none';
            if (actualContent) actualContent.style.display = 'flex';
            
            document.getElementById('searchInput').value = '';
            await this.loadChatHistory(res.chat_id, res.title);
            this.loadChatInfo(res.chat_id);
            
            const headerName = document.getElementById('headerName');
            if (headerName) headerName.textContent = res.title;
        } else {
            showToast('Route failed: ' + (res.reason || 'Unknown'), 'error');
        }
    },
    
    // --- Member Profile ---
    openMemberProfile(member) {
        const panel = document.getElementById('memberDetailPanel');
        if (!panel) return;
        
        panel.style.display = 'flex';
        const nameEl = document.getElementById('memberDetailName');
        const usernameEl = document.getElementById('memberDetailUsername');
        const bioEl = document.getElementById('memberDetailBio');
        const avatarEl = document.getElementById('memberDetailAvatar');
        
        const fullName = `${member.first_name || member.name || ''} ${member.last_name || ''}`.trim() || 'Unknown';
        if (nameEl) nameEl.textContent = fullName;
        if (usernameEl) usernameEl.textContent = member.username ? `@${member.username}` : 'No username';
        if (bioEl) bioEl.textContent = member.status || member.role || 'No bio';
        if (avatarEl) {
            avatarEl.textContent = getInitials(fullName);
            avatarEl.style.background = 'linear-gradient(135deg,var(--accent-blue),var(--secondary-blue))';
            if (member.photo) {
                avatarEl.innerHTML = `<img src="${member.photo}" style="width:100%;height:100%;border-radius:50%;object-fit:cover;">`;
            }
        }
    }
};

// ===================================================================
// 6. CONTACTS MANAGER
// ===================================================================
const ContactsManager = {
    contacts: [],
    
    async openContactsList() {
        if (!TG_STATE.selectedPhone) {
            showToast('Select a session first', 'warning');
            return;
        }
        
        const modal = document.getElementById('contactsListModal');
        if (!modal) return;
        modal.classList.add('show');
        
        const container = document.getElementById('contactsListContainer');
        if (!container) return;
        
        container.innerHTML = '<div style="text-align:center;padding:40px;color:var(--accent-blue);"><i class="fas fa-spinner fa-spin" style="font-size:24px;display:block;margin-bottom:10px;"></i>Loading contacts...</div>';
        
        const res = await TG_API.getContacts(TG_STATE.selectedPhone);
        if (res.status === 'success' && res.contacts) {
            this.contacts = res.contacts;
            this.renderContacts(res.contacts);
        } else {
            container.innerHTML = '<div style="text-align:center;padding:20px;color:var(--error);">Failed to load contacts</div>';
        }
    },
    
    renderContacts(contacts) {
        const container = document.getElementById('contactsListContainer');
        if (!container) return;
        
        if (!contacts || contacts.length === 0) {
            container.innerHTML = '<div style="text-align:center;padding:30px;color:var(--text-secondary);"><i class="fas fa-address-book" style="font-size:32px;display:block;margin-bottom:10px;opacity:0.3;"></i>No contacts found</div>';
            return;
        }
        
        container.innerHTML = '';
        
        // Group by first letter
        const grouped = {};
        contacts.forEach(c => {
            const letter = (c.first_name || c.username || '?').charAt(0).toUpperCase();
            if (!grouped[letter]) grouped[letter] = [];
            grouped[letter].push(c);
        });
        
        Object.keys(grouped).sort().forEach(letter => {
            const header = document.createElement('div');
            header.className = 'contact-letter-header';
            header.textContent = letter;
            container.appendChild(header);
            
            grouped[letter].forEach((c, index) => {
                const div = document.createElement('div');
                div.className = 'contact-item fade-in-up';
                div.style.animationDelay = `${(index % 10) * 0.03}s`;
                
                const fullName = `${c.first_name || ''} ${c.last_name || ''}`.trim() || c.username || 'Unknown';
                const phoneText = c.phone ? `+${c.phone}` : (c.username ? `@${c.username}` : '');
                
                div.innerHTML = `
                    <div class="contact-avatar">${getInitials(fullName)}</div>
                    <div class="contact-details">
                        <span class="contact-name">${escapeHtml(fullName)}</span>
                        <span class="contact-phone">${escapeHtml(phoneText)}</span>
                    </div>
                `;
                div.onclick = () => {
                    this.closeContactsList();
                    AccountManager.openChat(c.id, fullName, 'private');
                    showToast(`Opening chat with ${fullName}`);
                };
                container.appendChild(div);
            });
        });
    },
    
    filterContacts(query) {
        if (!query) {
            this.renderContacts(this.contacts);
            return;
        }
        const q = query.toLowerCase();
        const filtered = this.contacts.filter(c => {
            const name = `${c.first_name || ''} ${c.last_name || ''}`.toLowerCase();
            return name.includes(q) || (c.phone && c.phone.includes(q)) || (c.username && c.username.toLowerCase().includes(q));
        });
        this.renderContacts(filtered);
    },
    
    closeContactsList() {
        const modal = document.getElementById('contactsListModal');
        if (modal) modal.classList.remove('show');
    }
};

// ===================================================================
// 7. NEW MESSAGE COMPOSER
// ===================================================================
const NewMessageManager = {
    contacts: [],
    
    async openNewMessage() {
        if (!TG_STATE.selectedPhone) {
            showToast('Select a session first', 'warning');
            return;
        }
        const modal = document.getElementById('newMessageModal');
        if (!modal) return;
        modal.classList.add('show');
        
        const container = document.getElementById('newMessageContainer');
        if (!container) return;
        container.innerHTML = '<div style="text-align:center;padding:30px;color:var(--accent-blue);"><i class="fas fa-spinner fa-spin"></i> Loading...</div>';
        
        const res = await TG_API.getContacts(TG_STATE.selectedPhone);
        if (res.status === 'success' && res.contacts) {
            this.contacts = res.contacts;
            this.renderContactPicker(res.contacts);
        } else {
            container.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-secondary);">No contacts found</div>';
        }
        
        const search = document.getElementById('newMessageSearch');
        if (search) {
            search.value = '';
            search.oninput = debounce((e) => this.filterContactPicker(e.target.value), 300);
        }
    },
    
    renderContactPicker(contacts) {
        const container = document.getElementById('newMessageContainer');
        if (!container) return;
        if (!contacts.length) {
            container.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-secondary);">No contacts found</div>';
            return;
        }
        container.innerHTML = '';
        contacts.forEach(c => {
            const fullName = `${c.first_name || ''} ${c.last_name || ''}`.trim() || c.username || 'Unknown';
            const div = document.createElement('div');
            div.className = 'contact-item';
            div.innerHTML = `
                <div class="contact-avatar" style="background:var(--accent-blue);">${getInitials(fullName)}</div>
                <div class="contact-details">
                    <span class="contact-name">${escapeHtml(fullName)}</span>
                    <span class="contact-phone">${c.username ? '@' + escapeHtml(c.username) : (c.phone ? '+' + escapeHtml(c.phone) : '')}</span>
                </div>`;
            div.onclick = () => {
                document.getElementById('newMessageModal')?.classList.remove('show');
                AccountManager.openChat(c.id, fullName, 'private');
            };
            container.appendChild(div);
        });
    },
    
    filterContactPicker(query) {
        if (!query) return this.renderContactPicker(this.contacts);
        const q = query.toLowerCase();
        const filtered = this.contacts.filter(c => {
            const name = `${c.first_name || ''} ${c.last_name || ''}`.toLowerCase();
            return name.includes(q) || (c.username && c.username.toLowerCase().includes(q)) || (c.phone && c.phone.includes(q));
        });
        this.renderContactPicker(filtered);
    },
    
    async sendNewMessage() {
        const search = document.getElementById('newMessageSearch');
        const target = search?.value.trim();
        if (!target) {
            showToast('Enter a username, phone or link', 'warning');
            return;
        }
        const routeRes = await TG_API.smartRoute(TG_STATE.selectedPhone, target);
        if (routeRes.status === 'success') {
            document.getElementById('newMessageModal')?.classList.remove('show');
            await AccountManager.openChat(routeRes.chat_id, routeRes.title || target, 'private');
        } else {
            showToast('Route failed: ' + (routeRes.reason || 'Unknown'), 'error');
        }
    }
};

// ===================================================================
// 8. FOLDERS MANAGEMENT
// ===================================================================
const FoldersManager = {
    folders: [
        { id: 'all', name: 'All Chats', icon: 'fa-comments', filter: 'all' },
        { id: 'personal', name: 'Personal', icon: 'fa-user', filter: 'private' },
        { id: 'groups', name: 'Groups', icon: 'fa-users', filter: 'group' },
        { id: 'channels', name: 'Channels', icon: 'fa-bullhorn', filter: 'channel' },
        { id: 'archive', name: 'Archive', icon: 'fa-archive', filter: 'archive' }
    ],
    
    renderFolders() {
        const tabContainer = document.getElementById('folderTabs');
        if (!tabContainer) return;
        
        tabContainer.innerHTML = '<button class="folder-tab active" data-folder="all">All</button>';
        this.folders.filter(f => f.id !== 'all').forEach(folder => {
            const btn = document.createElement('button');
            btn.className = 'folder-tab';
            btn.dataset.folder = folder.filter;
            btn.textContent = folder.name;
            btn.onclick = () => this.selectFolder(folder);
            tabContainer.appendChild(btn);
        });

        const modalContainer = document.getElementById('foldersContainer');
        if (modalContainer) {
            modalContainer.innerHTML = '';
            this.folders.forEach(folder => {
                const div = document.createElement('div');
                div.className = 'folder-item';
                div.innerHTML = `<i class="fas ${folder.icon}"></i><span>${folder.name}</span>`;
                div.onclick = () => {
                    this.selectFolder(folder);
                    document.getElementById('foldersModal')?.classList.remove('show');
                };
                modalContainer.appendChild(div);
            });
        }
    },
    
    selectFolder(folder) {
        document.querySelectorAll('.folder-tab').forEach(el => {
            el.classList.toggle('active', el.dataset.folder === folder.filter || (folder.id === 'all' && el.dataset.folder === 'all'));
        });
        
        TG_STATE.activeFilter = folder.filter;
        AccountManager.renderDialogs(TG_STATE.dialogsCache, folder.filter);
    },
    
    openFoldersModal() {
        const modal = document.getElementById('foldersModal');
        if (modal) modal.classList.add('show');
    }
};

// ===================================================================
// 9. SETTINGS MANAGER
// ===================================================================
const SettingsManager = {
    openSettings() {
        const modal = document.getElementById('settingsModal');
        if (modal) modal.classList.add('show');
        this.renderSettingsPage('general');
        
        document.querySelectorAll('.settings-nav-item').forEach(item => {
            item.onclick = () => {
                document.querySelectorAll('.settings-nav-item').forEach(i => i.classList.remove('active'));
                item.classList.add('active');
                this.renderSettingsPage(item.dataset.page);
            };
        });
    },
    
    renderSettingsPage(page) {
        const content = document.getElementById('settingsContent');
        if (!content) return;
        const pages = {
            general: `<h3>General</h3><p style="color:var(--text-secondary);margin-top:8px;">Theme: ${TG_STATE.theme}</p>`,
            notifications: `<h3>Notifications</h3><p style="color:var(--text-secondary);margin-top:8px;">Notification settings</p>`,
            privacy: `<h3>Privacy & Security</h3><p style="color:var(--text-secondary);margin-top:8px;">Privacy controls</p>`,
            appearance: `<h3>Appearance</h3><div class="settings-item" style="margin-top:12px;"><span>Night Mode</span><button class="btn-secondary" onclick="toggleTheme()">Toggle</button></div>`,
            language: `<h3>Language</h3><p style="color:var(--text-secondary);margin-top:8px;">English</p>`,
            accounts: `<h3>Account Management</h3><p style="color:var(--text-secondary);margin-top:8px;">${AccountManager.getTotalCount()} sessions loaded</p>`,
            premium: `<h3>Premium</h3><p style="color:var(--text-secondary);margin-top:8px;">Premium features enabled</p>`,
            about: `<h3>About</h3><p style="color:var(--text-secondary);margin-top:8px;">Telegram Web Premium Suite</p>`
        };
        content.innerHTML = pages[page] || pages.general;
    },
    
    closeSettings() {
        document.getElementById('settingsModal')?.classList.remove('show');
    },
    
    toggleSetting(setting, value) {
        localStorage.setItem(`tg_setting_${setting}`, value);
        showToast(`${setting} updated`, 'success');
    },
    
    logout() {
        if (confirm('Logout from this session?')) {
            TG_STATE.selectedPhone = null;
            TG_STATE.currentChatId = null;
            TG_STATE.dialogsCache = [];
            TG_STATE.chatHistoryCache = {};
            TG_STATE.connectionStatus = 'disconnected';
            
            document.getElementById('chatList').innerHTML = '<div style="padding:30px;text-align:center;color:var(--text-secondary);">Select a session to begin</div>';
            const placeholder = document.getElementById('chatWindowPlaceholder');
            const actual = document.getElementById('chatWindowActualContent');
            if (placeholder) placeholder.style.display = 'flex';
            if (actual) actual.style.display = 'none';
            const messages = document.getElementById('messages');
            if (messages) messages.innerHTML = '';
            
            const avatarEl = document.querySelector('.user-avatar');
            const nameEl = document.querySelector('.user-info h3');
            if (avatarEl) { avatarEl.innerHTML = '<i class="fas fa-user"></i>'; avatarEl.style.background = 'var(--accent-blue)'; }
            if (nameEl) nameEl.textContent = 'No Session';
            
            this.closeSettings();
            showToast('Logged out', 'info');
        }
    }
};

// ===================================================================
// 10. ANALYTICS ENGINE
// ===================================================================
const AnalyticsEngine = {
    data: {},
    
    async openAnalytics() {
        if (!TG_STATE.selectedPhone) {
            showToast('Select a session first', 'warning');
            return;
        }
        
        const modal = document.getElementById('sessionAnalyticsModal');
        if (!modal) return;
        modal.classList.add('show');
        
        // Set loading states
        document.getElementById('analyticsPhone').textContent = `+${TG_STATE.selectedPhone}`;
        document.getElementById('analyticsDC').textContent = 'Loading...';
        document.getElementById('analyticsRoute').textContent = 'Loading...';
        document.getElementById('analyticsSpam').textContent = 'Loading...';
        
        try {
            const profileRes = await TG_API.getProfile(TG_STATE.selectedPhone);
            const healthRes = await TG_API.getSessionHealth(TG_STATE.selectedPhone);
            
            if (profileRes.status === 'success') {
                document.getElementById('analyticsDC').textContent = profileRes.dc_id || 'Unknown';
                document.getElementById('analyticsRoute').textContent = profileRes.proxy || 'Direct';
                document.getElementById('analyticsSpam').textContent = profileRes.restricted || 'Good';
                document.getElementById('analyticsSpam').style.color = (profileRes.restricted || '').includes('Good') ? 'var(--success)' : 'var(--error)';
            }
            
            const analyticsRes = await TG_API.getAnalytics(TG_STATE.selectedPhone);
            if (analyticsRes.status === 'success') {
                document.getElementById('analyticsTotalSessions').textContent = analyticsRes.total_sessions || 0;
                document.getElementById('analyticsActive').textContent = analyticsRes.active_sessions || 0;
            }
            
            if (healthRes.status === 'success') {
                // Render health charts/detailed info
                this.renderHealthData(healthRes);
            }
        } catch (err) {
            console.error('Analytics error:', err);
            document.getElementById('analyticsSpam').textContent = 'Error';
            document.getElementById('analyticsSpam').style.color = 'var(--error)';
        }
    },
    
    renderHealthData(data) {
        // This would render charts and detailed health metrics
        document.getElementById('analyticsAudit').textContent = data.details || 'All systems operational';
    },
    
    async openMassOperations() {
        if (!TG_STATE.selectedPhone) {
            showToast('Select a session first', 'warning');
            return;
        }
        
        const modal = document.getElementById('massOperationsModal');
        if (modal) modal.classList.add('show');
        
        // Start polling logs
        this.startLogPolling();
    },
    
    logPollInterval: null,
    
    startLogPolling() {
        if (this.logPollInterval) return;
        this.logPollInterval = setInterval(async () => {
            const container = document.getElementById('massLogOutput');
            if (!container) return;
            
            const res = await TG_API.getAutomationLogs();
            if (res.status === 'success' && res.logs) {
                container.textContent = res.logs.join('\n');
                container.scrollTop = container.scrollHeight;
            }
        }, 2000);
    },
    
    stopLogPolling() {
        if (this.logPollInterval) {
            clearInterval(this.logPollInterval);
            this.logPollInterval = null;
        }
    },
    
    async executeMassOps() {
        const input = document.getElementById('massTargetInput');
        const target = input?.value.trim();
        if (!target) {
            showToast('Enter a target', 'warning');
            return;
        }
        
        const btn = document.getElementById('executeBatchBtn');
        if (btn) { btn.disabled = true; btn.textContent = 'Executing...'; }
        
        const res = await TG_API.massExecute(target);
        if (res.status === 'success') {
            showToast('Batch execution started', 'success');
            if (input) input.value = '';
        } else {
            showToast('Execution failed: ' + (res.reason || 'Error'), 'error');
        }
        
        if (btn) { btn.disabled = false; btn.textContent = 'Execute Batch'; }
    }
};

// ===================================================================
// 11. EMOJI / STICKER / GIF PANEL
// ===================================================================
const EmojiPanel = {
    categories: ['emoji', 'sticker', 'gif'],
    activeCategory: 'emoji',
    
    data: {
        emoji: ['😀','😁','😂','🤣','😃','😄','😅','😆','😉','😊','😋','😎','😍','🥰','😘','😗','😙','😚','🙂','🤗','🤩','🤔','🤨','😐','😑','😶','🙄','😏','😣','😥','😮','🤐','😯','😪','😫','😴','😌','😛','😜','😝','🤤','😒','😓','😔','😕','🙃','🤑','😲','☹️','🙁','😖','😞','😟','😤','😢','😭','😦','😧','😨','😩','🤯','😬','😰','😱','🥵','🥶','😳','🤪','😵','😡','😠','🤬','👍','👎','👊','✊','🤛','🤜','👏','🙌','👐','🤲','🤝','🙏','✍️','💅','👀','👅','❤️','💔','💖','💙','💚','💛','🧡','💜','🖤','💝','💘','💞','💕','✨','⭐','🌟','🔥','💯','🎉','🎊','🎁','🎈','💎','🚀','💪','🏆','🍕','🍔','🌮','🍩','☕','🍺','🍻','🥂'],
        sticker: ['🔥','💯','🚀','✨','⭐','💪','🎉','🎊','❤️','💔','💖','👍','👎','👏','🙌','🎯','🏆','💎','🌟','⚡','💫','🎨','🎭','🎪','🎤','🎧','🎸','🎹','🎺','🎻','🥁','🎬','🎮','🎲','♟️','🎯','🏀','⚽','⚾','🎾','🏐','🏈','🏉','🎱','🏓','🏸','🥊','🥋','⛳','🎣','🏹','🎿','🛷','⛸️','🏂','🏋️','🤼','🤸','🤺','⛹️','🤾','🏌️','🏄','🏊','🤽','🚣','🧗','🚴','🚵','🤳','💃','🕺','🕴️','👯','🧖','🧘','🛀','🛌'],
        gif: ['🎥','🎬','🎭','🎨','🎪','🎤','🎧','🎸','🎹','🎺','🎻','🥁','🎮','🎲','♟️','🎯','🏀','⚽','⚾','🎾','🏐','🏈','🎱','🏓','🏸','🥊','🥋','⛳','🎣','🏹','🎿','🛷','🏂','🏋️','🤼','🤸','🤺','⛹️','🤾','🏌️','🏄','🏊','🤽','🚣','🧗','🚴','🚵','🤳','💃','🕺']
    },
    
    toggle() {
        const panel = document.querySelector('.emoji-panel');
        if (panel) {
            const isOpen = panel.classList.toggle('show');
            if (isOpen) this.render('emoji');
        }
    },
    
    render(category) {
        this.activeCategory = category;
        const grid = document.querySelector('.emoji-grid');
        if (!grid) return;
        
        const items = this.data[category] || [];
        grid.innerHTML = items.map(item => `<div class="emoji-item">${item}</div>`).join('');
        
        grid.querySelectorAll('.emoji-item').forEach(el => {
            el.addEventListener('click', () => {
                const input = document.getElementById('messageInput');
                if (input) {
                    input.value += el.textContent;
                    input.focus();
                    AccountManager.toggleSendButton();
                }
            });
        });
        
        // Update tabs
        document.querySelectorAll('.emoji-tab').forEach(tab => {
            tab.classList.toggle('active', tab.dataset.tab === category);
        });
    }
};

// ===================================================================
// 12. CALL MANAGER
// ===================================================================
const CallManager = {
    activeCall: null,
    
    startCall(type = 'voice', userId = null) {
        if (!TG_STATE.currentChatId) {
            showToast('Open a chat first', 'warning');
            return;
        }
        
        const modal = document.getElementById('callModal');
        if (modal) modal.classList.add('show');
        
        const nameEl = document.getElementById('callName');
        const statusEl = document.getElementById('callStatus');
        if (nameEl) nameEl.textContent = TG_STATE.currentChatTitle || 'Unknown';
        if (statusEl) statusEl.textContent = `Connecting ${type} call...`;
        
        this.activeCall = { type, contact: TG_STATE.currentChatTitle, startTime: Date.now() };
        
        setTimeout(() => {
            if (statusEl) statusEl.textContent = 'Calling...';
        }, 1500);
    },
    
    endCall() {
        this.activeCall = null;
        const modal = document.getElementById('callModal');
        if (modal) modal.classList.remove('show');
        showToast('Call ended', 'info');
    },
    
    toggleMute() {
        const btn = document.querySelector('#callModal .call-action-btn i.fa-microphone-slash, #callModal .call-action-btn i.fa-microphone');
        if (btn) btn.className = btn.className.includes('fa-microphone-slash') ? 'fas fa-microphone' : 'fas fa-microphone-slash';
    },
    
    toggleSpeaker() {
        showToast('Speaker toggled', 'info');
    }
};

// ===================================================================
// 13. DRAFT & SAVED MESSAGES
// ===================================================================
const DraftManager = {
    drafts: {},
    
    saveDraft(chatId, text) {
        if (text) {
            this.drafts[chatId] = { text, timestamp: Date.now() };
            localStorage.setItem(`tg_draft_${chatId}`, text);
        } else {
            delete this.drafts[chatId];
            localStorage.removeItem(`tg_draft_${chatId}`);
        }
    },
    
    loadDraft(chatId) {
        return this.drafts[chatId]?.text || localStorage.getItem(`tg_draft_${chatId}`) || '';
    },
    
    openSavedMessages() {
        AccountManager.openChat(TG_STATE.selectedPhone, 'Saved Messages', 'saved');
    }
};

// ===================================================================
// 14. STORIES VIEWER
// ===================================================================
const StoriesManager = {
    openStories() {
        showToast('Stories feature coming soon', 'info');
    }
};

// ===================================================================
// 15. WALLET (Telegram Stars & Crypto)
// ===================================================================
const WalletManager = {
    openWallet() {
        showToast('Telegram Wallet - Coming soon', 'info');
    }
};

// ===================================================================
// 16. GLOBAL EVENT BINDER & DOM INITIALIZATION
// ===================================================================
// Export to window for HTML onclick handlers
window.TG = {
    // Account
    selectSession: (phone) => AccountManager.selectAccount(phone),
    
    // Chat
    openChat: (chatId, title, type) => AccountManager.openChat(chatId, title, type),
    smartRoute: (target) => AccountManager.smartRoute(target),
    
    // Messages
    sendMessage: () => AccountManager.sendTextMessage(),
    replyToMessage: (id, text) => AccountManager.replyToMessage(id, text),
    clearReply: () => AccountManager.clearReply(),
    editMessage: (id, text) => AccountManager.editMessage(id, text),
    forwardMessage: (id) => AccountManager.forwardMessage(id),
    deleteMessage: (id) => AccountManager.deleteMessage(id),
    confirmDelete: (forEveryone) => AccountManager.confirmDelete(forEveryone),
    pinMessage: (id) => AccountManager.pinMessage(id),
    
    // Contacts
    openContacts: () => ContactsManager.openContactsList(),
    
    // New Message
    openNewMessage: () => NewMessageManager.openNewMessage(),
    sendNewMessage: () => NewMessageManager.sendNewMessage(),
    
    // Folders
    openFolders: () => FoldersManager.openFoldersModal(),
    selectFolder: (id) => FoldersManager.selectFolder(FoldersManager.folders.find(f => f.id === id)),
    
    // Settings
    openSettings: () => SettingsManager.openSettings(),
    closeSettings: () => SettingsManager.closeSettings(),
    toggleTheme: () => toggleTheme(),
    logout: () => SettingsManager.logout(),
    
    // Analytics
    openAnalytics: () => AnalyticsEngine.openAnalytics(),
    openMassOps: () => AnalyticsEngine.openMassOperations(),
    executeMassOps: () => AnalyticsEngine.executeMassOps(),
    
    // Emoji
    toggleEmoji: () => EmojiPanel.toggle(),
    selectEmojiCategory: (cat) => EmojiPanel.render(cat),
    
    // Calls
    startCall: (type) => CallManager.startCall(type),
    endCall: () => CallManager.endCall(),
    toggleMute: () => CallManager.toggleMute(),
    toggleSpeaker: () => CallManager.toggleSpeaker(),
    
    // Saved Messages
    openSavedMessages: () => DraftManager.openSavedMessages(),
    
    // Stories
    openStories: () => StoriesManager.openStories(),
    
    // Wallet
    openWallet: () => WalletManager.openWallet(),
    
    // Member profile
    openMemberProfile: (member) => AccountManager.openMemberProfile(member),
    
    // Utilities
    closeModal: (id) => {
        const modal = document.getElementById(id);
        if (modal) modal.classList.remove('show');
        // Stop analytics polling if closing mass ops modal
        if (id === 'massOperationsModal') AnalyticsEngine.stopLogPolling();
    },
    
    // Search
    searchGlobal: () => {
        const input = document.getElementById('searchInput');
        const query = input?.value.trim();
        if (!query) return;
        
        if (query.includes('t.me') || query.includes('+') || query.startsWith('@')) {
            TG.smartRoute(query);
        } else {
            TG.openNewMessage();
            const search = document.getElementById('newMessageSearch');
            if (search) search.value = query;
        }
    },
    
    // Proxies
    toggleSearch: () => {
        const input = document.getElementById('searchInput');
        input?.focus();
    },
    
    // Account Management
    refreshAccounts: () => AccountManager.loadAccounts().then(accounts => {
        AccountManager.accounts = accounts;
        // Re-render sidebar
        const menu = document.getElementById('accountSessionsList') || document.querySelector('.menu');
        if (menu) {
            const phoneCount = accounts.length;
            menu.innerHTML = `
                <div class="menu-item active" style="font-weight:bold;color:var(--accent-blue);cursor:default;padding-bottom:5px;">
                    <i class="fas fa-user-shield"></i><span>SESSIONS (${phoneCount})</span>
                </div>
            `;
            accounts.forEach((acc, idx) => {
                const div = document.createElement('div');
                div.className = `menu-item ${TG_STATE.selectedPhone === acc.phone ? 'active' : ''}`;
                div.onclick = () => AccountManager.selectAccount(acc.phone);
                const dotColor = acc.status === 'active' ? 'var(--success)' : 'var(--error)';
                div.innerHTML = `
                    <span style="font-size:11px;font-weight:bold;color:var(--secondary-blue);min-width:24px;opacity:0.8;">#${idx + 1}</span>
                    <i class="fas fa-circle" style="font-size:8px;color:${dotColor};margin-right:6px;flex-shrink:0;"></i>
                    <div style="display:flex;flex-direction:column;flex:1;min-width:0;line-height:1.3;">
                        <span style="font-weight:600;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${acc.first_name || 'Account'}</span>
                        <span style="font-size:11px;color:var(--text-secondary);">+${acc.phone}</span>
                    </div>
                `;
                menu.appendChild(div);
            });
        }
    })
};

window.closeModal = (id) => TG.closeModal(id);

// ===================================================================
// 17. DOM READY — MAIN INITIALIZATION
// ===================================================================
document.addEventListener('DOMContentLoaded', async () => {
    console.log('🚀 Telegram Web K Premium Engine initializing...');
    
    // #region agent log
    const domChecks = {
        sendBtn: !!document.getElementById('sendBtn'),
        messages: !!document.getElementById('messages'),
        replyPreview: !!document.getElementById('replyPreview'),
        toggleEmoji: !!document.getElementById('toggleEmoji'),
        attachBtn: !!document.getElementById('attachBtn'),
        callModal: !!document.getElementById('callModal'),
        infoMembersList: !!document.getElementById('infoMembersList'),
        accountSessionsList: !!document.getElementById('accountSessionsList')
    };
    debugCompatLog('script.js:DOMContentLoaded', 'DOM element compatibility check', domChecks, 'H1');
    // #endregion
    
    applyTheme(TG_STATE.theme);
    FoldersManager.renderFolders();
    await TG.refreshAccounts();
    
    const lastPhone = sessionStorage.getItem('tg_selected_phone');
    if (lastPhone && AccountManager.accountsMap[lastPhone]) {
        AccountManager.selectAccount(lastPhone);
    }
    
    const sendBtn = document.getElementById('sendBtn');
    const msgInput = document.getElementById('messageInput');
    
    if (sendBtn) {
        sendBtn.addEventListener('click', () => {
            if (msgInput && msgInput.value.trim()) {
                AccountManager.sendTextMessage();
            } else {
                showToast('Hold to record voice message', 'info');
            }
        });
    }
    
    if (msgInput) {
        msgInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                AccountManager.sendTextMessage();
            }
        });
        msgInput.addEventListener('input', () => {
            AccountManager.toggleSendButton();
            DraftManager.saveDraft(TG_STATE.currentChatId, msgInput.value);
        });
        if (TG_STATE.currentChatId) {
            msgInput.value = DraftManager.loadDraft(TG_STATE.currentChatId);
            AccountManager.toggleSendButton();
        }
    }
    
    document.getElementById('toggleEmoji')?.addEventListener('click', () => EmojiPanel.toggle());
    document.getElementById('emojiPanelClose')?.addEventListener('click', () => {
        document.getElementById('emojiPanel')?.classList.remove('show');
    });
    document.getElementById('attachBtn')?.addEventListener('click', () => {
        showToast('Attachment options: Photo, Video, File, Poll, Location', 'info');
    });
    document.getElementById('replyPreviewClose')?.addEventListener('click', () => AccountManager.clearReply());
    
    document.getElementById('openSessionAnalytics')?.addEventListener('click', () => AnalyticsEngine.openAnalytics());
    document.getElementById('openContactsList')?.addEventListener('click', () => ContactsManager.openContactsList());
    document.getElementById('openMassOperations')?.addEventListener('click', () => AnalyticsEngine.openMassOperations());
    document.getElementById('openFolders')?.addEventListener('click', () => FoldersManager.openFoldersModal());
    document.getElementById('openSettings')?.addEventListener('click', () => SettingsManager.openSettings());
    document.getElementById('toggleNightMode')?.addEventListener('click', toggleTheme);
    document.getElementById('logoutBtn')?.addEventListener('click', () => SettingsManager.logout());
    document.getElementById('newMessageBtn')?.addEventListener('click', () => NewMessageManager.openNewMessage());
    
    document.getElementById('openInfoPanel')?.addEventListener('click', () => {
        document.getElementById('infoPanel')?.classList.add('show');
    });
    document.getElementById('infoPanelClose')?.addEventListener('click', () => {
        document.getElementById('infoPanel')?.classList.remove('show');
    });
    document.getElementById('voiceCallBtn')?.addEventListener('click', () => CallManager.startCall('voice'));
    document.getElementById('videoCallBtn')?.addEventListener('click', () => CallManager.startCall('video'));
    document.getElementById('memberDetailBack')?.addEventListener('click', () => {
        document.getElementById('memberDetailPanel').style.display = 'none';
    });
    
    const sidebar = document.getElementById('sidebar');
    const overlay = document.getElementById('overlay');
    document.getElementById('mobileMenuToggle')?.addEventListener('click', () => {
        sidebar?.classList.toggle('open');
        overlay?.classList.toggle('show');
    });
    overlay?.addEventListener('click', () => {
        sidebar?.classList.remove('open');
        overlay?.classList.remove('show');
        document.getElementById('infoPanel')?.classList.remove('show');
        document.getElementById('emojiPanel')?.classList.remove('show');
    });
    document.getElementById('mobileChatBack')?.addEventListener('click', () => {
        document.body.classList.remove('chat-active');
        document.getElementById('chatListPanel')?.classList.remove('hide');
        TG_STATE.currentChatId = null;
        document.getElementById('chatWindowPlaceholder').style.display = 'flex';
        document.getElementById('chatWindowActualContent').style.display = 'none';
    });
    
    const searchInput = document.getElementById('searchInput');
    if (searchInput) {
        searchInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') { e.preventDefault(); TG.searchGlobal(); }
        });
        searchInput.addEventListener('input', debounce((e) => {
            TG_STATE.searchQuery = e.target.value.trim();
            AccountManager.renderDialogs(TG_STATE.dialogsCache, TG_STATE.activeFilter);
        }, 300));
    }
    
    document.querySelectorAll('.filter-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('.filter-tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            TG_STATE.activeFilter = tab.dataset.filter || 'all';
            AccountManager.renderDialogs(TG_STATE.dialogsCache, TG_STATE.activeFilter);
        });
    });
    
    document.getElementById('forwardChatSearchInput')?.addEventListener('input', debounce((e) => {
        AccountManager.renderForwardChatList(e.target.value);
    }, 300));
    
    document.getElementById('contactSearchInput')?.addEventListener('input', debounce((e) => {
        ContactsManager.filterContacts(e.target.value);
    }, 300));
    
    document.getElementById('confirmMessageDeleteBtn')?.addEventListener('click', () => {
        AccountManager.confirmDelete(document.getElementById('deleteEveryoneCheckbox')?.checked || false);
    });
    
    document.getElementById('executeBatchBtn')?.addEventListener('click', () => AnalyticsEngine.executeMassOps());
    
    document.querySelectorAll('.emoji-tab').forEach(tab => {
        tab.addEventListener('click', () => EmojiPanel.render(tab.dataset.tab || 'emoji'));
    });
    
    document.addEventListener('keydown', (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
            e.preventDefault();
            document.getElementById('searchInput')?.focus();
        }
        if (e.key === 'Escape') {
            document.querySelectorAll('.modal.show').forEach(m => m.classList.remove('show'));
            document.getElementById('emojiPanel')?.classList.remove('show');
            document.getElementById('infoPanel')?.classList.remove('show');
            AnalyticsEngine.stopLogPolling();
        }
    });
    
    const statusIndicator = document.createElement('div');
    statusIndicator.className = 'connection-status';
    statusIndicator.style.cssText = 'width:8px;height:8px;border-radius:50%;background:var(--error);position:fixed;bottom:10px;right:10px;z-index:9999;';
    statusIndicator.title = 'Disconnected';
    document.body.appendChild(statusIndicator);
    
    // #region agent log
    debugCompatLog('script.js:DOMContentLoaded', 'Initialization complete', { accounts: AccountManager.accounts.length, theme: TG_STATE.theme }, 'H1');
    // #endregion
    
    console.log('✅ Telegram Web K Premium Engine initialized successfully');
    console.log(`📊 Account pool: ${AccountManager.accounts.length} sessions`);
});

// ===================================================================
// 18. MEMBER DETAIL PANEL
// ===================================================================
(function enhanceInfoPanel() {
    document.getElementById('memberDetailMessage')?.addEventListener('click', () => {
        document.getElementById('memberDetailPanel').style.display = 'none';
        showToast('Opening chat...', 'info');
    });
})();

// ===================================================================
// 19. AUTO-SCROLL & INFINITE SCROLL (Chat History)
// ===================================================================
(function setupInfiniteScroll() {
    const messagesContainer = document.getElementById('messages');
    if (!messagesContainer) return;
    
    messagesContainer.addEventListener('scroll', throttle(() => {
        // If scrolled to top, load older messages
        if (messagesContainer.scrollTop < 50 && TG_STATE.currentChatId) {
            // Load more (pagination)
            // This would call loadMoreMessages in production
        }
    }, 500));
})();

// ===================================================================
// 20. WINDOW RESIZE HANDLER (Responsive Mappings Fixed)
// ===================================================================
window.addEventListener('resize', throttle(() => {
    const width = window.innerWidth;
    // 🔥 FIX: Changed to correct premium HTML panel classes
    const sidebarPanel = document.getElementById('sidebar');
    const chatListPanel = document.getElementById('chatListPanel');
    const chatWindow = document.getElementById('chatWindow');
    const rightPanel = document.getElementById('infoPanel');
    
    if (width < 768) {
        if (chatListPanel) chatListPanel.style.display = TG_STATE.currentChatId ? 'none' : 'flex';
        if (rightPanel) rightPanel.style.display = 'none';
    } else if (width < 1024) {
        if (sidebarPanel) sidebarPanel.style.display = 'flex';
        if (chatListPanel) chatListPanel.style.display = 'flex';
        if (rightPanel) rightPanel.style.display = 'none';
    } else {
        if (sidebarPanel) sidebarPanel.style.display = 'flex';
        if (chatListPanel) chatListPanel.style.display = 'flex';
        if (rightPanel) rightPanel.style.display = TG_STATE.currentChatId ? 'flex' : 'none';
    }
}, 200));

// ===================================================================
// 21. PREMIUM SUBSCRIPTION HANDLER
// ===================================================================
(function initPremiumFeatures() {
    if (TG_STATE.isPremium) {
        console.log('✨ Premium features unlocked');
        document.querySelectorAll('.premium-only').forEach(el => el.style.display = '');
    }
})();

// ===================================================================
// 22. BACKGROUND SYNC & HEALTH CHECK POLL
// ===================================================================
setInterval(async () => {
    if (TG_STATE.selectedPhone) {
        try {
            const res = await TG_API.get(`/api/console/ping/${TG_STATE.selectedPhone}`);
            if (res.status === 'success') {
                TG_STATE.connectionStatus = 'connected';
            }
        } catch (e) {
            TG_STATE.connectionStatus = 'disconnected';
        }
        AccountManager.refreshConnectionStatus();
    }
}, 30000); // Every 30 seconds

// ===================================================================
// 23. FAVICON UNREAD BADGE UPDATER
// ===================================================================
setInterval(() => {
    let totalUnread = 0;
    TG_STATE.dialogsCache.forEach(d => { totalUnread += d.unread_count || 0; });
    
    const link = document.querySelector('link[rel="icon"]');
    if (link && totalUnread > 0) {
        // Would update favicon with badge count
    }
}, 15000);

// ===================================================================
// 24. CONTEXT MENU (Right-click on messages)
// ===================================================================
document.addEventListener('contextmenu', (e) => {
    const messageEl = e.target.closest('.message');
    if (messageEl) {
        e.preventDefault();
        const msgId = messageEl.dataset.msgId;
        const contextMenu = document.getElementById('contextMenu');
        if (contextMenu) {
            contextMenu.style.display = 'block';
            contextMenu.style.left = `${e.clientX}px`;
            contextMenu.style.top = `${e.clientY}px`;
            contextMenu.dataset.msgId = msgId;
        }
    }
});

document.addEventListener('click', () => {
    const contextMenu = document.getElementById('contextMenu');
    if (contextMenu) contextMenu.style.display = 'none';
});

console.log('🚀 Telegram Web K Premium — Script loaded successfully');
console.log('📱 Ready for 1000+ accounts with full analytics');