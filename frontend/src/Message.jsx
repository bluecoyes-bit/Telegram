import { formatTime, normalizeMessage } from "./api";

function TextLinks({ text, onRoute }) {
  if (!text) return null;
  const parts = [];
  const re = /(https?:\/\/[^\s]+)|(@[a-zA-Z0-9_]{5,32})/gi;
  let last = 0;
  let match;
  while ((match = re.exec(text))) {
    if (match.index > last) parts.push(text.slice(last, match.index));
    const token = match[0];
    parts.push(
      <button
        key={`${match.index}-${token}`}
        type="button"
        className="inline-link"
        onClick={(e) => {
          e.stopPropagation();
          onRoute?.(token.replace(/^@/, ""));
        }}
      >
        {token}
      </button>
    );
    last = match.index + token.length;
  }
  if (last < text.length) parts.push(text.slice(last));
  return <div className="msg-text">{parts}</div>;
}

function MediaBlock({ media, onPhoto }) {
  if (!media) return null;
  const type = media.type || "";
  if (type === "photo" && media.url) {
    return <img className="msg-photo" src={media.url} alt="" onClick={() => onPhoto?.(media.url, media.caption)} />;
  }
  if (type === "video" && media.url) {
    return <video className="msg-photo" src={media.url} controls poster={media.thumb || ""} />;
  }
  if ((type === "audio" || type === "voice") && media.url) {
    return <audio className="msg-audio" src={media.url} controls />;
  }
  if (type === "sticker" && media.url) {
    return <img className="msg-sticker" src={media.url} alt="" />;
  }
  if (type === "gif" && media.url) {
    return <img className="msg-photo" src={media.url} alt="" />;
  }
  if (type === "link" && media.url) {
    return (
      <a className="msg-file" href={media.url} target="_blank" rel="noreferrer" onClick={(e) => e.stopPropagation()}>
        <b>{media.title || media.url}</b>
        <small>{media.url}</small>
      </a>
    );
  }
  return (
    <div className="msg-file">
      <b>{media.filename || type || "Media"}</b>
      <small>{media.size || "Attachment"}</small>
    </div>
  );
}

export default function Message({ msg, onReply, onForward, onEdit, onDelete, onPin, onRoute, onPhoto }) {
  const m = normalizeMessage(msg);
  return (
    <div className={`bubble ${m.outgoing ? "out" : "in"}`}>
      {m.forward_from ? <div className="msg-sub">Forwarded from {m.forward_from}</div> : null}
      {m.reply_to_msg_id ? <div className="msg-sub">↩ {m.reply_to_sender || "Reply"}: {m.reply_to_text}</div> : null}
      <MediaBlock media={m.media} onPhoto={onPhoto} />
      {m.poll ? (
        <div className="poll">
          <b>{m.poll.question}</b>
          {(m.poll.options || []).map((opt) => (
            <div key={opt.text} className="poll-opt">{opt.text}</div>
          ))}
        </div>
      ) : null}
      <TextLinks text={m.text} onRoute={onRoute} />
      <div className="meta">
        {formatTime(m.date)}
        {m.outgoing ? " ✓" : ""}
        {m.edited ? " · edited" : ""}
      </div>
      <div className="hover">
        <button type="button" title="Reply" onClick={() => onReply(m)}>↩</button>
        <button type="button" title="Forward" onClick={() => onForward(m)}>➔</button>
        {m.outgoing ? <button type="button" title="Edit" onClick={() => onEdit(m)}>✎</button> : null}
        <button type="button" title="Pin" onClick={() => onPin(m)}>📌</button>
        <button type="button" title="Delete" onClick={() => onDelete(m)}>🗑</button>
      </div>
    </div>
  );
}
