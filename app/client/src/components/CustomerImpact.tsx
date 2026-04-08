import React, { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchDMAProperties } from "../api";
import LoadingSpinner from "./common/LoadingSpinner";
import EmptyState from "./common/EmptyState";

const IMPACT_COLORS: Record<string, string> = {
  high: "#DC2626",
  medium: "#F59E0B",
  low: "#16A34A",
  none: "#94A3B8",
};

function classifyImpact(property: any, simulatedPressurePct: number) {
  const height = Number(property.elevation_m ?? property.height ?? 0);
  const basePressure = Number(property.base_pressure ?? 25);
  const simPressure = basePressure * (simulatedPressurePct / 100);
  const effectivePressure = simPressure - height * 0.098;
  if (effectivePressure <= 0) return "high";
  if (effectivePressure < 5) return "medium";
  if (effectivePressure < 10) return "low";
  return "none";
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
      return { ...p, impact };
    });
    classified.sort(
      (a: any, b: any) =>
        Number(b.elevation_m ?? b.height ?? 0) - Number(a.elevation_m ?? a.height ?? 0)
    );
    return { classified, counts };
  }, [properties, pressurePct]);

  if (isLoading) return <LoadingSpinner message="Loading properties..." />;
  if (properties.length === 0) {
    return <EmptyState title="No properties" message="No property data available for this DMA." />;
  }

  const { classified, counts } = impactSummary;
  const total = properties.length;

  return (
    <div className="space-y-4">
      {/* What-if slider */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-3">
        <div className="flex items-center justify-between mb-2">
          <p className="text-xs font-medium text-blue-800">What-if: Simulated pressure level</p>
          <span className="text-sm font-semibold text-blue-700">{pressurePct}%</span>
        </div>
        <input
          type="range"
          min={0}
          max={100}
          value={pressurePct}
          onChange={(e) => setPressurePct(Number(e.target.value))}
          className="w-full h-2 bg-blue-200 rounded-lg appearance-none cursor-pointer"
        />
        <p className="text-xs text-blue-600 mt-1">
          Estimated impact — based on elevation and current pressure
        </p>
      </div>

      {/* Impact summary */}
      <div className="grid grid-cols-4 gap-2 text-center text-xs">
        {([
          { key: "high", label: "High", color: "bg-red-100 text-red-800" },
          { key: "medium", label: "Medium", color: "bg-amber-100 text-amber-800" },
          { key: "low", label: "Low", color: "bg-green-100 text-green-800" },
          { key: "none", label: "None", color: "bg-gray-100 text-gray-600" },
        ] as const).map((cat) => (
          <div key={cat.key} className={`rounded-lg p-2 ${cat.color}`}>
            <p className="font-bold text-lg">{counts[cat.key]}</p>
            <p>{cat.label}</p>
          </div>
        ))}
      </div>

      <p className="text-xs text-gray-500 text-center">
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
              <span className="text-gray-400 ml-2">{p.property_type || ""}</span>
              {(p.elevation_m ?? p.height) != null && (
                <span className="text-gray-400 ml-2">
                  {Number(p.elevation_m ?? p.height).toFixed(0)}m elev
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
