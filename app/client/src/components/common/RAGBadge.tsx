const STATUS_CONFIG: Record<string, { bg: string; text: string; dot: string }> = {
  RED: { bg: "bg-red-100", text: "text-red-800", dot: "bg-red-500" },
  AMBER: { bg: "bg-amber-100", text: "text-amber-800", dot: "bg-amber-500" },
  GREEN: { bg: "bg-green-100", text: "text-green-800", dot: "bg-green-500" },
};

export default function RAGBadge({
  status,
  label,
  className = "",
  pulse = false,
}: {
  status?: string;
  label?: string;
  className?: string;
  pulse?: boolean;
}) {
  const s = status?.toUpperCase() || "GREEN";
  const cfg = STATUS_CONFIG[s] || STATUS_CONFIG.GREEN;
  const shouldPulse = pulse && s === "RED";
  return (
    <span className={`badge ${cfg.bg} ${cfg.text} ${className}`}>
      <span
        className={`w-2.5 h-2.5 rounded-full ${cfg.dot} mr-1.5 ring-1 ring-inset ring-black/10 ${
          shouldPulse ? "animate-pulse" : ""
        }`}
      />
      {label || s}
    </span>
  );
}
