import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchDMAProperties } from "../api";
import { humanize } from "../utils/format";
import LoadingSpinner from "./common/LoadingSpinner";
import EmptyState from "./common/EmptyState";

const IMPACT_COLORS: Record<string, string> = {
  high: "#DC2626",
  medium: "#F59E0B",
  low: "#16A34A",
  none: "#94A3B8",
};

function classifyImpact(property: any, simulatedPressurePct: number) {
  // Sensitive premises (hospitals, schools, care homes, dialysis) are always high impact
  if (property.is_sensitive_premise || property.sensitive_premise_type) return "high";

  const height = Number(property.customer_height_m ?? property.elevation_m ?? property.height ?? 0);
  const basePressure = Number(property.base_pressure ?? 25);
  const simPressure = basePressure * (simulatedPressurePct / 100);
  const effectivePressure = simPressure - height * 0.098;
  if (effectivePressure <= 0) return "high";
  if (effectivePressure < 5) return "medium";
  if (effectivePressure < 10) return "low";
  return "none";
}

const PRESETS = [
  { label: "50% of normal", value: 50 },
  { label: "Full outage (0%)", value: 0 },
  { label: "Fully restored (100%)", value: 100 },
];

export default function CustomerImpact({ dmaCode }: { dmaCode: string }) {
  const [pressurePct, setPressurePct] = useState(50);

  const { data, isLoading } = useQuery({
    queryKey: ["dmaProperties", dmaCode],
    queryFn: () => fetchDMAProperties(dmaCode),
    enabled: !!dmaCode,
  });

  const properties = data?.properties || [];

  const impactSummary = useMemo(() => {
    const counts = { high: 0, medium: 0, low: 0, none: 0 };
    const classified = properties.map((p: any) => {
      const impact = classifyImpact(p, pressurePct);
      counts[impact as keyof typeof counts]++;
      return { ...p, impact };
    });
    classified.sort(
      (a: any, b: any) =>
        Number(b.customer_height_m ?? b.elevation_m ?? b.height ?? 0) - Number(a.customer_height_m ?? a.elevation_m ?? a.height ?? 0)
    );
    return { classified, counts };
  }, [properties, pressurePct]);

  if (isLoading) return <LoadingSpinner message="Loading properties..." />;
  if (properties.length === 0) {
    return <EmptyState title="No properties" message="No property data available for this DMA." />;
  }

  const { classified, counts } = impactSummary;
  const total = properties.length;

  const HIGH_THRESHOLD = 10;

  return (
    <div className="space-y-4">
      {/* What-if slider with tick marks and presets */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-3">
        <div className="flex items-center justify-between mb-2">
          <p className="text-xs font-medium text-blue-800">What-if: Pressure as % of normal</p>
          <span className="text-sm font-semibold text-blue-700 tabular-nums">{pressurePct}% of normal</span>
        </div>
        <div className="relative">
          <input
            type="range"
            min={0}
            max={100}
            value={pressurePct}
            onChange={(e) => setPressurePct(Number(e.target.value))}
            className="w-full h-2 bg-blue-200 rounded-lg appearance-none cursor-pointer"
          />
          {/* Tick marks */}
          <div className="flex justify-between mt-1 px-0.5">
            {[0, 25, 50, 75, 100].map((v) => (
              <span key={v} className="text-[9px] text-blue-400 tabular-nums">{v}%</span>
            ))}
          </div>
        </div>
        {/* Preset buttons */}
        <div className="flex gap-2 mt-2">
          {PRESETS.map((p) => (
            <button
              key={p.value}
              onClick={() => setPressurePct(p.value)}
              className={`px-2 py-1 rounded text-[10px] font-medium transition-colors ${
                pressurePct === p.value
                  ? "bg-blue-600 text-white"
                  : "bg-white text-blue-700 border border-blue-200 hover:bg-blue-50"
              }`}
            >
              {p.label}
            </button>
          ))}
        </div>
      </div>

      {/* Impact summary cards with glow on high impact */}
      <div className="grid grid-cols-4 gap-2 text-center text-xs">
        {([
          { key: "high", label: "High", color: "bg-red-100 text-red-800" },
          { key: "medium", label: "Medium", color: "bg-amber-100 text-amber-800" },
          { key: "low", label: "Low", color: "bg-green-100 text-green-800" },
          { key: "none", label: "None", color: "bg-gray-100 text-gray-600" },
        ] as const).map((cat) => (
          <div
            key={cat.key}
            className={`rounded-lg p-2 ${cat.color} ${
              cat.key === "high" && counts.high > HIGH_THRESHOLD
                ? "ring-2 ring-red-400"
                : ""
            }`}
          >
            <p className="font-bold text-lg tabular-nums">{counts[cat.key]}</p>
            <p>{cat.label}</p>
          </div>
        ))}
      </div>

      <p className="text-xs text-gray-500 text-center tabular-nums">
        {total} properties total — {counts.high + counts.medium} impacted at {pressurePct}% pressure
      </p>

      {/* Property list */}
      <div className="space-y-1.5 max-h-60 overflow-y-auto">
        {classified.slice(0, 30).map((p: any, i: number) => (
          <div
            key={p.property_id || i}
            className="flex items-center justify-between bg-gray-50 rounded-lg px-3 py-2 text-xs"
          >
            <div>
              <span className="font-medium">{p.property_id || `Property ${i + 1}`}</span>
              <span className="text-gray-400 ml-2">{humanize(p.property_type) || ""}</span>
              {(p.customer_height_m ?? p.elevation_m ?? p.height) != null && (
                <span className="text-gray-400 ml-2 tabular-nums">
                  {Number(p.customer_height_m ?? p.elevation_m ?? p.height).toFixed(0)}m elev
                </span>
              )}
            </div>
            <span
              className="w-3 h-3 rounded-full flex-shrink-0"
              style={{ backgroundColor: IMPACT_COLORS[p.impact] }}
              title={p.impact}
            />
          </div>
        ))}
        {classified.length > 30 && (
          <p className="text-xs text-gray-400 text-center py-1">
            +{classified.length - 30} more properties
          </p>
        )}
      </div>
    </div>
  );
}
