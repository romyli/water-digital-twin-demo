import React from "react";

const STATUS_CONFIG: Record<string, { bg: string; text: string; dot: string }> = {
  RED: { bg: "bg-red-100", text: "text-red-800", dot: "bg-red-500" },
  AMBER: { bg: "bg-amber-100", text: "text-amber-800", dot: "bg-amber-500" },
  GREEN: { bg: "bg-green-100", text: "text-green-800", dot: "bg-green-500" },
};

export default function RAGBadge({
  status,
  label,
  className = "",
}: {
  status?: string;
  label?: string;
  className?: string;
}) {
  const s = status?.toUpperCase() || "GREEN";
  const cfg = STATUS_CONFIG[s] || STATUS_CONFIG.GREEN;
  return (
    <span className={`badge ${cfg.bg} ${cfg.text} ${className}`}>
      <span className={`w-2 h-2 rounded-full ${cfg.dot} mr-1.5`} />
      {label || s}
    </span>
  );
}
