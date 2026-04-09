import { useState } from "react";

const RAG_COLORS: Record<string, string> = {
  RED: "#DC2626",
  AMBER: "#F59E0B",
  GREEN: "#16A34A",
};

export default function TimelineStrip({
  history = [],
  className = "",
}: {
  history?: any[];
  className?: string;
}) {
  const [hoverIdx, setHoverIdx] = useState<number | null>(null);

  if (!history.length) {
    return (
      <div className={`h-8 bg-gray-100 rounded text-xs text-gray-400 flex items-center justify-center ${className}`}>
        No RAG history
      </div>
    );
  }

  const total = history.length;
  const first = history[0];
  const startLabel = first.recorded_at
    ? new Date(first.recorded_at).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit" })
    : "00:00";
  const hoverEntry = hoverIdx != null ? history[hoverIdx] : null;

  return (
    <div className={className}>
      <div className="relative">
        <div className="flex h-8 rounded overflow-hidden" title="RAG Timeline">
          {history.map((entry, i) => {
            const color = RAG_COLORS[entry.rag_status?.toUpperCase()] || RAG_COLORS.GREEN;
            return (
              <div
                key={i}
                style={{
                  backgroundColor: color,
                  width: `${100 / total}%`,
                  minWidth: "2px",
                }}
                className="relative"
                onMouseEnter={() => setHoverIdx(i)}
                onMouseLeave={() => setHoverIdx(null)}
              />
            );
          })}
        </div>
        {/* Current-time needle */}
        <div className="absolute right-0 top-0 bottom-0 w-0.5 bg-white shadow-sm" />

        {/* Hover tooltip */}
        {hoverEntry && hoverIdx != null && (
          <div
            className="absolute -top-8 z-20 bg-gray-900 text-white text-[10px] rounded px-2 py-1 whitespace-nowrap pointer-events-none shadow-lg"
            style={{
              left: `${((hoverIdx + 0.5) / total) * 100}%`,
              transform: "translateX(-50%)",
            }}
          >
            {hoverEntry.recorded_at
              ? new Date(hoverEntry.recorded_at).toLocaleTimeString("en-GB", {
                  hour: "2-digit",
                  minute: "2-digit",
                })
              : ""}{" "}
            — {hoverEntry.rag_status}
          </div>
        )}
      </div>
      {/* Start/end labels */}
      <div className="flex justify-between mt-0.5">
        <span className="text-[10px] text-gray-400 tabular-nums">{startLabel}</span>
        <span className="text-[10px] text-gray-400">now</span>
      </div>
    </div>
  );
}
