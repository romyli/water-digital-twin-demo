import { useState, useRef, useEffect, type FormEvent } from "react";
import { useGenieChat } from "@databricks/appkit-ui/react";

/* ---------- Query result helpers ---------- */

function QueryResultTable({ result }: { result: any }) {
  const columns: any[] = result?.manifest?.schema?.columns ?? result?.columns ?? [];
  const rows: any[][] = result?.result?.data_array ?? result?.data ?? [];
  if (!columns.length || !rows.length) return null;

  return (
    <div className="overflow-x-auto rounded-lg border border-gray-200 mt-2">
      <table className="min-w-full text-xs">
        <thead>
          <tr className="bg-gray-50 border-b border-gray-200">
            {columns.map((col: any, i: number) => (
              <th key={i} className="px-3 py-2 text-left font-semibold text-gray-600 whitespace-nowrap">
                {col.name ?? col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 100).map((row, ri) => (
            <tr key={ri} className={ri % 2 === 0 ? "bg-white" : "bg-gray-50/50"}>
              {row.map((cell: any, ci: number) => (
                <td key={ci} className="px-3 py-1.5 text-gray-700 whitespace-nowrap border-t border-gray-100">
                  {cell ?? "—"}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > 100 && (
        <div className="px-3 py-1.5 text-xs text-gray-400 bg-gray-50 border-t">
          Showing 100 of {rows.length} rows
        </div>
      )}
    </div>
  );
}

function SqlBlock({ sql, description }: { sql?: string; description?: string }) {
  const [open, setOpen] = useState(false);
  if (!sql) return null;

  return (
    <div className="mt-2 rounded-lg border border-gray-200 bg-gray-50 text-xs">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center gap-2 px-3 py-2 text-gray-600 font-medium hover:bg-gray-100 transition-colors"
      >
        <span className={`transition-transform ${open ? "rotate-90" : ""}`}>&#9654;</span>
        SQL Query
      </button>
      {open && (
        <div className="px-3 pb-3 border-t border-gray-200">
          {description && <p className="text-gray-500 mt-2 mb-1">{description}</p>}
          <pre className="mt-1 p-2 rounded bg-gray-900 text-green-300 text-[11px] whitespace-pre-wrap break-all overflow-x-auto">
            {sql}
          </pre>
        </div>
      )}
    </div>
  );
}

/* ---------- Message bubble ---------- */

function MessageBubble({ message, isStreaming }: { message: any; isStreaming?: boolean }) {
  const isUser = message.role === "user";

  // Extract query results from the Map
  const queryEntries: [string, any][] = [];
  if (message.queryResults && typeof message.queryResults.forEach === "function") {
    message.queryResults.forEach((val: any, key: string) => {
      queryEntries.push([key, val]);
    });
  }

  const showTyping = !isUser && isStreaming && !message.content && queryEntries.length === 0;

  return (
    <div className={`flex gap-3 ${isUser ? "justify-end" : "justify-start"}`}>
      {/* AI avatar */}
      {!isUser && (
        <div className="w-8 h-8 rounded-full bg-gray-600 text-white flex items-center justify-center text-xs font-bold shrink-0 mt-1">
          AI
        </div>
      )}

      <div className={`flex flex-col gap-2 max-w-[75%] min-w-0 ${isUser ? "items-end" : "items-start"}`}>
        {/* Typing dots (empty assistant message while streaming) */}
        {showTyping && (
          <div className="rounded-2xl rounded-bl-md px-4 py-3 bg-white border border-gray-200 shadow-sm">
            <div className="flex gap-1.5">
              <span className="w-2 h-2 rounded-full bg-gray-400 animate-bounce" style={{ animationDelay: "0ms" }} />
              <span className="w-2 h-2 rounded-full bg-gray-400 animate-bounce" style={{ animationDelay: "150ms" }} />
              <span className="w-2 h-2 rounded-full bg-gray-400 animate-bounce" style={{ animationDelay: "300ms" }} />
            </div>
          </div>
        )}

        {/* Text bubble */}
        {message.content && (
          <div
            className={`rounded-2xl px-4 py-2.5 text-sm leading-relaxed ${
              isUser
                ? "bg-water-700 text-white rounded-br-md"
                : "bg-white border border-gray-200 text-gray-800 rounded-bl-md shadow-sm"
            }`}
            dangerouslySetInnerHTML={{ __html: formatContent(message.content) }}
          />
        )}

        {/* Query results (AI only) */}
        {queryEntries.map(([key, result]) => (
          <div key={key} className="w-full">
            <SqlBlock sql={result?.query ?? result?.sql} description={result?.description} />
            <QueryResultTable result={result} />
          </div>
        ))}

        {/* Error state */}
        {message.error && (
          <div className="rounded-lg px-3 py-2 text-xs bg-red-50 border border-red-200 text-red-700">
            {message.error}
          </div>
        )}
      </div>

      {/* User avatar */}
      {isUser && (
        <div className="w-8 h-8 rounded-full bg-water-700 text-white flex items-center justify-center text-xs font-bold shrink-0 mt-1">
          You
        </div>
      )}
    </div>
  );
}

/* ---------- Simple markdown-ish formatting ---------- */

function formatContent(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(/\*(.+?)\*/g, "<em>$1</em>")
    .replace(/`(.+?)`/g, '<code class="px-1 py-0.5 bg-gray-100 rounded text-xs">$1</code>')
    .replace(/\n/g, "<br />");
}


/* ---------- Empty state ---------- */

function EmptyState() {
  return (
    <div className="flex flex-col items-center justify-center h-full text-center px-4">
      <div className="w-16 h-16 rounded-full bg-water-100 flex items-center justify-center mb-4">
        <svg className="w-8 h-8 text-water-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M8.625 12a.375.375 0 1 1-.75 0 .375.375 0 0 1 .75 0Zm0 0H8.25m4.125 0a.375.375 0 1 1-.75 0 .375.375 0 0 1 .75 0Zm0 0H12m4.125 0a.375.375 0 1 1-.75 0 .375.375 0 0 1 .75 0Zm0 0h-.375M21 12c0 4.556-4.03 8.25-9 8.25a9.764 9.764 0 0 1-2.555-.337A5.972 5.972 0 0 1 5.41 20.97a5.969 5.969 0 0 1-.474-.065 4.48 4.48 0 0 0 .978-2.025c.09-.457-.133-.901-.467-1.226C3.93 16.178 3 14.189 3 12c0-4.556 4.03-8.25 9-8.25s9 3.694 9 8.25Z" />
        </svg>
      </div>
      <h2 className="text-lg font-semibold text-gray-800 mb-1">Network Operations Assistant</h2>
      <p className="text-sm text-gray-500 max-w-sm">
        Ask questions about DMA health, sensor telemetry, pressure trends, anomaly scores, and asset status.
      </p>
      <div className="mt-6 flex flex-wrap gap-2 justify-center">
        {["Which DMAs are red?", "Show pressure trends", "List active incidents", "Anomaly scores today"].map((q) => (
          <span key={q} className="px-3 py-1.5 rounded-full bg-white border border-gray-200 text-xs text-gray-600 cursor-default">
            {q}
          </span>
        ))}
      </div>
    </div>
  );
}

/* ---------- Page ---------- */

export default function GeniePage() {
  const { messages, status, sendMessage, reset } = useGenieChat({ alias: "default" });
  const [input, setInput] = useState("");
  const scrollRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  // Auto-scroll on new messages
  useEffect(() => {
    const el = scrollRef.current;
    if (el) el.scrollTo({ top: el.scrollHeight, behavior: "smooth" });
  }, [messages, status]);

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    const trimmed = input.trim();
    if (!trimmed || status === "streaming") return;
    sendMessage(trimmed);
    setInput("");
    inputRef.current?.focus();
  };

  const isStreaming = status === "streaming";

  return (
    <div className="flex flex-col h-[calc(100vh-3.5rem)] bg-gray-50">
      {/* Header bar */}
      <div className="flex items-center justify-between px-6 py-2.5 border-b border-gray-200 bg-white shrink-0">
        <div className="flex items-center gap-3">
          <div className="w-7 h-7 rounded-lg bg-water-700 text-white flex items-center justify-center">
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M8.625 12a.375.375 0 1 1-.75 0 .375.375 0 0 1 .75 0Zm0 0H8.25m4.125 0a.375.375 0 1 1-.75 0 .375.375 0 0 1 .75 0Zm0 0H12m4.125 0a.375.375 0 1 1-.75 0 .375.375 0 0 1 .75 0Zm0 0h-.375M21 12c0 4.556-4.03 8.25-9 8.25a9.764 9.764 0 0 1-2.555-.337A5.972 5.972 0 0 1 5.41 20.97a5.969 5.969 0 0 1-.474-.065 4.48 4.48 0 0 0 .978-2.025c.09-.457-.133-.901-.467-1.226C3.93 16.178 3 14.189 3 12c0-4.556 4.03-8.25 9-8.25s9 3.694 9 8.25Z" />
            </svg>
          </div>
          <div>
            <h1 className="text-sm font-semibold text-gray-900">Network Operations Assistant</h1>
            <p className="text-[11px] text-gray-500">Powered by Databricks Genie</p>
          </div>
        </div>
        <button
          onClick={reset}
          className="text-xs text-gray-500 hover:text-gray-700 px-3 py-1.5 rounded-lg hover:bg-gray-100 transition-colors"
        >
          New conversation
        </button>
      </div>

      {/* Messages area (scrollable) */}
      <div ref={scrollRef} className="flex-1 min-h-0 overflow-y-auto">
        {messages.length === 0 && !isStreaming ? (
          <EmptyState />
        ) : (
          <div className="max-w-3xl mx-auto px-6 py-4 space-y-4">
            {messages.map((msg: any, i: number) => (
              <MessageBubble key={msg.id} message={msg} isStreaming={isStreaming && i === messages.length - 1} />
            ))}
          </div>
        )}
      </div>

      {/* Input bar (fixed at bottom) */}
      <div className="border-t border-gray-200 bg-white px-6 py-3 shrink-0">
        <form onSubmit={handleSubmit} className="max-w-3xl mx-auto flex gap-3">
          <textarea
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                handleSubmit(e);
              }
            }}
            placeholder="Ask a question..."
            rows={1}
            className="flex-1 resize-none rounded-xl border border-gray-300 bg-gray-50 px-4 py-2.5 text-sm placeholder:text-gray-400 focus:outline-none focus:ring-2 focus:ring-water-600 focus:border-transparent transition-shadow"
          />
          <button
            type="submit"
            disabled={!input.trim() || isStreaming}
            className="px-4 py-2.5 rounded-xl bg-water-700 text-white text-sm font-medium hover:bg-water-600 disabled:opacity-40 disabled:cursor-not-allowed transition-colors shrink-0"
          >
            {isStreaming ? (
              <svg className="w-4 h-4 animate-spin" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
            ) : (
              "Send"
            )}
          </button>
        </form>
      </div>
    </div>
  );
}
