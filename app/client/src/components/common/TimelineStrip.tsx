import React from "react";

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
  if (!history.length) {
    return (
      <div className={`h-6 bg-gray-100 rounded text-xs text-gray-400 flex items-center justify-center ${className}`}>
        No RAG history
      </div>
    );
  }
  const total = history.length;
  return (
    <div className={`flex h-6 rounded overflow-hidden ${className}`} title="RAG Timeline">
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
            title={`${entry.recorded_at || ""}: ${entry.rag_status}`}
          />
        );
      })}
    </div>
  );
}
