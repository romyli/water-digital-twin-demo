import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchDMAProperties } from "../api";
import { humanize } from "../utils/format";
import {
  ResponsiveContainer,
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  Tooltip,
  ReferenceLine,
  CartesianGrid,
} from "recharts";
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

function ElevationTooltip({ active, payload }: any) {
  if (!active || !payload?.[0]) return null;
  const d = payload[0].payload;
  return (
    <div className="bg-gray-900 text-white rounded-lg px-3 py-2 text-xs shadow-xl">
      <p className="font-semibold">{d.property_id}</p>
      <p>{humanize(d.property_type)}</p>
      <p>Elevation: {d.height.toFixed(0)}m</p>
      <p>Effective pressure: {d.effectivePressure.toFixed(1)}m</p>
      <p>Impact: {d.impact}</p>
    </div>
  );
}

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
      const height = Number(p.customer_height_m ?? p.elevation_m ?? p.height ?? 0);
      const basePressure = Number(p.base_pressure ?? 25);
      const simPressure = basePressure * (pressurePct / 100);
      const effectivePressure = simPressure - height * 0.098;
      return { ...p, impact, height, effectivePressure };
    });
    classified.sort((a: any, b: any) => b.height - a.height);
    return { classified, counts };
  }, [properties, pressurePct]);

  if (isLoading) return <LoadingSpinner message="Loading properties..." />;
  if (properties.length === 0) {
    return <EmptyState title="No properties" message="No property data available for this DMA." />;
  }

  const { classified, counts } = impactSummary;
  const total = properties.length;

  const HIGH_THRESHOLD = 10;

  // Prepare scatter data grouped by impact level
  const scatterByImpact = {
    high: classified.filter((p: any) => p.impact === "high"),
    medium: classified.filter((p: any) => p.impact === "medium"),
    low: classified.filter((p: any) => p.impact === "low"),
    none: classified.filter((p: any) => p.impact === "none"),
  };

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
          <div className="flex justify-between mt-1 px-0.5">
            {[0, 25, 50, 75, 100].map((v) => (
              <span key={v} className="text-[9px] text-blue-400 tabular-nums">{v}%</span>
            ))}
          </div>
        </div>
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

      {/* Impact summary cards */}
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

      {/* Elevation vs Effective Pressure scatter chart */}
      <div className="bg-gray-50 rounded-lg p-3">
        <p className="text-xs font-medium text-gray-500 mb-2">Elevation vs Effective Pressure</p>
        <ResponsiveContainer width="100%" height={200}>
          <ScatterChart margin={{ top: 5, right: 5, bottom: 5, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis
              dataKey="height"
              type="number"
              name="Elevation"
              tick={{ fontSize: 10 }}
              label={{ value: "Elevation (m)", position: "insideBottom", offset: -2, style: { fontSize: 10 } }}
            />
            <YAxis
              dataKey="effectivePressure"
              type="number"
              name="Pressure"
              tick={{ fontSize: 10 }}
              label={{ value: "Eff. Pressure (m)", angle: -90, position: "insideLeft", style: { fontSize: 10 } }}
            />
            <Tooltip content={<ElevationTooltip />} />
            {/* Danger zones */}
            <ReferenceLine y={0} stroke="#DC2626" strokeDasharray="3 3" />
            <ReferenceLine y={5} stroke="#F59E0B" strokeDasharray="3 3" />
            <ReferenceLine y={10} stroke="#16A34A" strokeDasharray="3 3" />
            {scatterByImpact.high.length > 0 && (
              <Scatter name="High" data={scatterByImpact.high} fill={IMPACT_COLORS.high} r={3} />
            )}
            {scatterByImpact.medium.length > 0 && (
              <Scatter name="Medium" data={scatterByImpact.medium} fill={IMPACT_COLORS.medium} r={3} />
            )}
            {scatterByImpact.low.length > 0 && (
              <Scatter name="Low" data={scatterByImpact.low} fill={IMPACT_COLORS.low} r={2} />
            )}
            {scatterByImpact.none.length > 0 && (
              <Scatter name="None" data={scatterByImpact.none} fill={IMPACT_COLORS.none} r={2} />
            )}
          </ScatterChart>
        </ResponsiveContainer>
        <div className="flex items-center justify-center gap-3 mt-1 text-[10px] text-gray-500">
          <span>— <span className="text-red-500">0m</span> No supply</span>
          <span>— <span className="text-amber-500">5m</span> Low pressure</span>
          <span>— <span className="text-green-500">10m</span> Min acceptable</span>
        </div>
      </div>
    </div>
  );
}
