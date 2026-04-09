import { useState, useRef, useEffect, type FormEvent } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { fetchComms, addComms } from "../api";
import { format } from "date-fns";
import LoadingSpinner from "./common/LoadingSpinner";
import EmptyState from "./common/EmptyState";

const CHANNEL_ICONS: Record<string, string> = {
  email: "\u2709",     // envelope
  phone: "\uD83D\uDCDE", // phone
  sms: "\uD83D\uDCAC",   // chat bubble
  radio: "\uD83D\uDCFB", // radio
  teams: "\uD83D\uDC65", // people
  system: "\u2699",       // gear
};

function isOutbound(entry: any) {
  const dir = entry.direction?.toLowerCase() || "";
  return dir.includes("out") || dir.includes("operator") || dir.includes("sent");
}

function isSystemMessage(entry: any) {
  return entry.channel === "system";
}

export default function CommunicationsLog({
  incidentId,
  initialComms,
}: {
  incidentId?: string;
  initialComms?: any[];
}) {
  const queryClient = useQueryClient();
  const [inputFocused, setInputFocused] = useState(false);
  const [formData, setFormData] = useState({
    channel: "email",
    recipient: "",
    message: "",
  });
  const scrollRef = useRef<HTMLDivElement>(null);

  const { data, isLoading } = useQuery({
    queryKey: ["comms", incidentId],
    queryFn: () => fetchComms(incidentId!),
    enabled: !!incidentId,
    placeholderData: initialComms ? { communications: initialComms } : undefined,
    refetchInterval: 10_000,
  });

  // Reverse so oldest is at top, most recent at bottom (chat-style chronological)
  const rawComms = data?.communications || initialComms || [];
  const comms = [...rawComms].reverse();

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [comms.length]);

  const mutation = useMutation({
    mutationFn: (entry: any) => addComms(incidentId!, entry),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["comms", incidentId] });
      queryClient.invalidateQueries({ queryKey: ["handover", incidentId] });
      setFormData({ channel: "email", recipient: "", message: "" });
      setInputFocused(false);
    },
  });

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    if (!formData.message.trim()) return;
    mutation.mutate(formData);
  };

  // Group messages by date for dividers
  let lastDate = "";

  return (
    <div className="card flex flex-col">
      <h2 className="panel-title mb-3">Communications Log</h2>

      {isLoading ? (
        <LoadingSpinner message="Loading communications..." />
      ) : comms.length === 0 ? (
        <EmptyState title="No communications" message="No communication records for this incident." />
      ) : (
        <div ref={scrollRef} className="space-y-1.5 max-h-72 overflow-y-auto mb-3 px-1">
          {comms.map((c: any, i: number) => {
            const ts = c.comms_timestamp ? new Date(c.comms_timestamp) : null;
            const dateStr = ts ? format(ts, "dd MMM") : "";
            const showDivider = dateStr && dateStr !== lastDate;
            if (dateStr) lastDate = dateStr;

            const system = isSystemMessage(c);
            const outbound = !system && isOutbound(c);
            const icon = CHANNEL_ICONS[c.channel] || "\uD83D\uDCAC";

            return (
              <div key={i}>
                {/* Date divider */}
                {showDivider && (
                  <div className="flex items-center gap-2 my-2">
                    <div className="flex-1 h-px bg-gray-200" />
                    <span className="text-[10px] text-gray-400">{dateStr}</span>
                    <div className="flex-1 h-px bg-gray-200" />
                  </div>
                )}

                {/* System message */}
                {system ? (
                  <div className="text-center">
                    <p className="text-gray-400 italic text-xs py-1">
                      {c.summary}
                      {ts && <span className="ml-2 text-[10px] not-italic tabular-nums">{format(ts, "HH:mm")}</span>}
                    </p>
                  </div>
                ) : (
                  /* Chat bubble */
                  <div className={`flex ${outbound ? "justify-end" : "justify-start"}`}>
                    <div
                      className={`max-w-[80%] rounded-lg px-3 py-2 text-sm ${
                        outbound ? "bg-water-50 ml-auto" : "bg-gray-50"
                      }`}
                    >
                      <div className="flex items-center gap-1.5 mb-0.5">
                        <span className="text-sm" title={c.channel}>{icon}</span>
                        {c.recipient_name && (
                          <span className="text-xs text-gray-500">{c.recipient_name}</span>
                        )}
                        <span className="text-[10px] text-gray-400 ml-auto tabular-nums">
                          {ts ? format(ts, "HH:mm") : ""}
                        </span>
                      </div>
                      <p className="text-gray-700 text-xs">{c.summary}</p>
                      {c.direction && (
                        <p className="text-gray-400 text-[10px] mt-0.5">by {c.direction}</p>
                      )}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}

      {/* Persistent compact input — expands on focus */}
      <form onSubmit={handleSubmit} className="border-t border-gray-100 pt-3">
        {inputFocused && (
          <div className="flex gap-2 mb-2">
            <select
              value={formData.channel}
              onChange={(e) => setFormData({ ...formData, channel: e.target.value })}
              className="text-xs border border-gray-200 rounded-lg px-2 py-1.5 bg-white"
            >
              <option value="email">Email</option>
              <option value="phone">Phone</option>
              <option value="sms">SMS</option>
              <option value="radio">Radio</option>
              <option value="teams">Teams</option>
              <option value="system">System</option>
            </select>
            <input
              type="text"
              placeholder="Recipient (optional)"
              value={formData.recipient}
              onChange={(e) => setFormData({ ...formData, recipient: e.target.value })}
              className="flex-1 text-xs border border-gray-200 rounded-lg px-3 py-1.5"
            />
          </div>
        )}
        <div className="flex gap-2">
          <input
            type="text"
            placeholder="Type a message..."
            value={formData.message}
            onChange={(e) => setFormData({ ...formData, message: e.target.value })}
            onFocus={() => setInputFocused(true)}
            className="flex-1 text-sm border border-gray-200 rounded-lg px-3 py-2"
          />
          <button
            type="submit"
            disabled={!formData.message.trim() || mutation.isPending}
            className="btn-primary text-xs px-3"
          >
            {mutation.isPending ? "..." : "Send"}
          </button>
        </div>
      </form>
    </div>
  );
}
