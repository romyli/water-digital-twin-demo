import { useState, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  fetchDMADetail,
  fetchDMASensors,
  fetchDMAComplaints,
  fetchDMAAssets,
  fetchDMARagHistory,
  fetchReservoirs,
} from "../api";
import { humanize } from "../utils/format";
import RAGBadge from "./common/RAGBadge";
import TimelineStrip from "./common/TimelineStrip";
import LoadingSpinner from "./common/LoadingSpinner";
import EmptyState from "./common/EmptyState";
import AssetDetail from "./AssetDetail";
import CustomerImpact from "./CustomerImpact";

/* ---------- Root Cause Chain Visualization ---------- */
function RootCauseChain({
  pumpAssets,
  trunkMains,
  dmaCode,
  downstreamCount,
  pumpRag,
  trunkRag,
}: {
  pumpAssets: any[];
  trunkMains: any[];
  dmaCode: string;
  downstreamCount?: number;
  pumpRag: string;
  trunkRag: string;
}) {
  const nodes = [
    ...(pumpAssets.length > 0
      ? [{ id: pumpAssets[0].asset_id, type: "Pump Station", rag: pumpRag }]
      : []),
    ...(trunkMains.length > 0
      ? [{ id: trunkMains[0].asset_id, type: "Trunk Main", rag: trunkRag }]
      : []),
    { id: dmaCode, type: "DMA", rag: "RED" },
    ...(downstreamCount != null && downstreamCount > 0
      ? [{ id: `${downstreamCount} downstream`, type: "Downstream DMAs", rag: "AMBER" }]
      : []),
  ];

  const ragBg: Record<string, string> = {
    RED: "bg-red-100 border-red-400 text-red-800",
    AMBER: "bg-amber-100 border-amber-400 text-amber-800",
    GREEN: "bg-green-100 border-green-400 text-green-800",
  };

  return (
    <div className="overflow-x-auto pb-1">
      <div className="flex items-center gap-0 py-2 min-w-min">
        {nodes.map((node, i) => (
          <div key={i} className="flex items-center flex-shrink-0">
            <div
              className={`rounded-lg border-2 px-2.5 py-1.5 text-center ${
                ragBg[node.rag?.toUpperCase()] || ragBg.GREEN
              }`}
              title={node.id}
            >
              <p className="text-[11px] font-bold truncate max-w-[80px]">{node.id}</p>
              <p className="text-[10px] opacity-70">{humanize(node.type)}</p>
            </div>
            {i < nodes.length - 1 && (
              <svg className="w-5 h-4 text-gray-400 flex-shrink-0" viewBox="0 0 20 16" fill="none" stroke="currentColor" strokeWidth={2}>
                <path d="M2 8h12M11 3l4 5-4 5" />
              </svg>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

/* ---------- Pressure Mini Bar ---------- */
function PressureBar({ value, max = 30 }: { value: number | null; max?: number }) {
  if (value == null) return <span className="text-xs text-gray-400">—</span>;
  const pct = Math.min(100, Math.max(0, (value / max) * 100));
  const isLow = value < 10;
  return (
    <div className="flex items-center gap-2">
      <div className="w-16 h-2 bg-gray-200 rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full ${isLow ? "bg-red-500" : "bg-green-500"}`}
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className={`text-xs font-medium tabular-nums ${isLow ? "text-red-600" : "text-gray-700"}`}>
        {Number(value).toFixed(1)}m
      </span>
    </div>
  );
}

export default function DMADetail({
  dmaCode,
  activeIncident,
}: {
  dmaCode: string;
  activeIncident: any;
}) {
  const [activeTab, setActiveTab] = useState("summary");
  const [selectedSensor, setSelectedSensor] = useState<string | null>(null);

  // Listen for Escape to close sensor detail
  useEffect(() => {
    const handler = () => setSelectedSensor(null);
    window.addEventListener("app:escape", handler);
    return () => window.removeEventListener("app:escape", handler);
  }, []);

  const { data: detail, isLoading: loadingDetail } = useQuery({
    queryKey: ["dmaDetail", dmaCode],
    queryFn: () => fetchDMADetail(dmaCode),
    enabled: !!dmaCode,
  });

  const { data: sensorsData, isLoading: loadingSensors, error: sensorsError } = useQuery({
    queryKey: ["dmaSensors", dmaCode],
    queryFn: () => fetchDMASensors(dmaCode),
    enabled: !!dmaCode,
    retry: 2,
  });

  const { data: complaintsData } = useQuery({
    queryKey: ["dmaComplaints", dmaCode],
    queryFn: () => fetchDMAComplaints(dmaCode),
    enabled: !!dmaCode,
  });

  const { data: assetsData } = useQuery({
    queryKey: ["dmaAssets", dmaCode],
    queryFn: () => fetchDMAAssets(dmaCode),
    enabled: !!dmaCode,
  });

  const { data: ragData } = useQuery({
    queryKey: ["dmaRag", dmaCode],
    queryFn: () => fetchDMARagHistory(dmaCode),
    enabled: !!dmaCode,
  });

  const { data: reservoirData } = useQuery({
    queryKey: ["reservoirs", dmaCode],
    queryFn: () => fetchReservoirs(dmaCode),
    enabled: !!dmaCode,
  });

  const sensors = sensorsData?.sensors || [];
  const complaints = complaintsData?.complaints || [];
  const assets = assetsData?.assets || [];
  const ragHistory = ragData?.rag_history || [];
  const reservoirs = reservoirData?.reservoirs || [];

  const sortedSensors = [...sensors].sort(
    (a: any, b: any) => (a.latest_pressure ?? 999) - (b.latest_pressure ?? 999)
  );

  // Infer asset RAG status from incident context when the data doesn't include it.
  // During an active incident, the root-cause pump station should show RED, not GREEN.
  const incidentType = activeIncident?.incident_type?.toLowerCase() || "";
  const inferAssetRag = (asset: any): string => {
    const existing = asset.rag_status?.toUpperCase();
    if (existing && existing !== "GREEN") return existing;
    // If no explicit status and there's an active incident, infer from asset type
    if (activeIncident) {
      if (asset.asset_type === "pump_station" && incidentType.includes("pump")) return "RED";
      if (asset.asset_type === "trunk_main") return "AMBER";
    }
    return existing || "GREEN";
  };

  const pumpAssets = assets.filter((a: any) => a.asset_type === "pump_station");
  const trunkMains = assets.filter((a: any) => a.asset_type === "trunk_main");
  const hasRootCause = pumpAssets.length > 0 || trunkMains.length > 0;

  if (loadingDetail) return <LoadingSpinner message="Loading DMA details..." />;

  if (selectedSensor) {
    return (
      <div className="p-4">
        <button
          onClick={() => setSelectedSensor(null)}
          className="text-sm text-water-600 hover:underline mb-3"
        >
          &larr; Back to DMA
        </button>
        <AssetDetail
          sensorId={selectedSensor}
          dmaCode={dmaCode}
          activeIncident={activeIncident}
          firstComplaintTime={complaints.length > 0 ? complaints[complaints.length - 1]?.complaint_timestamp : undefined}
        />
      </div>
    );
  }

  const tabs = [
    { key: "summary", label: "Summary" },
    { key: "sensors", label: `Sensors (${sensors.length})` },
    { key: "complaints", label: `Complaints (${complaints.length})` },
    { key: "impact", label: "Impact" },
  ];

  return (
    <div className="p-4 space-y-4">
      <div>
        <p className="text-xs text-gray-500 mb-1">RAG History (24h)</p>
        <TimelineStrip history={ragHistory} />
      </div>

      {/* Segmented Control Tabs */}
      <div className="bg-gray-100 rounded-lg p-0.5 flex gap-0.5">
        {tabs.map((t) => (
          <button
            key={t.key}
            onClick={() => setActiveTab(t.key)}
            className={`flex-1 px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
              activeTab === t.key
                ? "bg-white shadow-sm text-gray-900"
                : "text-gray-500 hover:text-gray-700"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {activeTab === "summary" && (
        <div className="space-y-4">
          {/* Colored stat cards */}
          <div className="grid grid-cols-2 gap-3 text-sm">
            {(() => {
              const avgP = detail?.avg_pressure != null ? Number(detail.avg_pressure) : null;
              const avgF = detail?.avg_flow != null ? Number(detail.avg_flow) : null;
              const pLow = avgP != null && avgP < 10;
              return (
                <>
                  <div className={`rounded-lg p-3 ${pLow ? "bg-red-50 border border-red-200" : "bg-gray-50"}`}>
                    <p className="text-gray-500 text-xs">Avg Pressure</p>
                    <p className={`font-semibold text-lg tabular-nums ${pLow ? "text-red-600" : ""}`}>
                      {avgP != null ? `${avgP.toFixed(1)} m` : "—"}
                    </p>
                  </div>
                  <div className="bg-gray-50 rounded-lg p-3">
                    <p className="text-gray-500 text-xs">Avg Flow</p>
                    <p className="font-semibold text-lg tabular-nums">
                      {avgF != null ? `${avgF.toFixed(1)} l/s` : "—"}
                    </p>
                  </div>
                </>
              );
            })()}
            <div className="bg-gray-50 rounded-lg p-3">
              <p className="text-gray-500 text-xs">Sensors</p>
              <p className="font-semibold text-lg tabular-nums">{detail?.sensor_count ?? sensors.length}</p>
            </div>
            <div className="bg-gray-50 rounded-lg p-3">
              <p className="text-gray-500 text-xs">Properties</p>
              <p className="font-semibold text-lg tabular-nums">{detail?.property_count ?? "—"}</p>
            </div>
          </div>

          {/* Root Cause Chain */}
          {hasRootCause && (
            <div>
              <p className="text-xs font-medium text-gray-500 mb-2">Upstream Root Cause</p>
              <RootCauseChain
                pumpAssets={pumpAssets}
                trunkMains={trunkMains}
                dmaCode={dmaCode}
                downstreamCount={detail?.downstream_dma_count}
                pumpRag={pumpAssets.length > 0 ? inferAssetRag(pumpAssets[0]) : "RED"}
                trunkRag={trunkMains.length > 0 ? inferAssetRag(trunkMains[0]) : "AMBER"}
              />
              {detail?.total_properties != null && (
                <p className="text-xs text-gray-400 mt-1">
                  ~{detail.total_properties} properties affected
                </p>
              )}
            </div>
          )}

          {reservoirs.length > 0 && (
            <div>
              <p className="text-xs font-medium text-gray-500 mb-2">Reservoir Status</p>
              {reservoirs.map((r: any) => (
                <div key={r.reservoir_id} className="bg-gray-50 rounded-lg p-3 text-sm">
                  <div className="flex justify-between items-center">
                    <span className="font-medium">{r.reservoir_id}</span>
                    <RAGBadge
                      status={
                        (r.level_pct ?? 50) < 30 ? "RED" : (r.level_pct ?? 50) < 60 ? "AMBER" : "GREEN"
                      }
                      label={`${r.level_pct ?? "—"}%`}
                    />
                  </div>
                  {r.est_supply_hours != null && (
                    <p className="text-gray-500 text-xs mt-1">
                      Est. {Number(r.est_supply_hours).toFixed(1)}h supply remaining
                    </p>
                  )}
                </div>
              ))}
            </div>
          )}

          {assets.length > 0 && (
            <div>
              <p className="text-xs font-medium text-gray-500 mb-2">Upstream Assets</p>
              <div className="space-y-1.5">
                {assets.map((a: any) => (
                  <div
                    key={a.asset_id}
                    className="flex items-center justify-between bg-gray-50 rounded-lg px-3 py-2 text-sm"
                  >
                    <div>
                      <span className="font-medium">{a.asset_id}</span>
                      <span className="text-gray-500 ml-2 text-xs">{humanize(a.asset_type)}</span>
                    </div>
                    <RAGBadge status={inferAssetRag(a)} />
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {activeTab === "sensors" && (
        <div className="space-y-1.5">
          {loadingSensors ? (
            <LoadingSpinner message="Loading sensors..." />
          ) : sensorsError ? (
            <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm text-red-700">
              <p className="font-medium">Failed to load sensors</p>
              <p className="text-xs mt-1">{(sensorsError as Error).message}</p>
            </div>
          ) : sortedSensors.length === 0 ? (
            <EmptyState title="No sensors" message="No sensors found in this DMA." />
          ) : (
            sortedSensors.map((s: any) => {
              const pressure = s.latest_pressure ?? s.avg_pressure;
              return (
                <button
                  key={s.sensor_id}
                  onClick={() => setSelectedSensor(s.sensor_id)}
                  className="w-full text-left bg-gray-50 hover:bg-gray-100 rounded-lg px-3 py-2.5 text-sm transition-colors"
                >
                  <div className="flex items-center justify-between">
                    <div>
                      <span className="font-medium">{s.sensor_id}</span>
                      {s.sensor_type && <span className="text-xs text-gray-400 ml-2">{s.sensor_type}</span>}
                    </div>
                    <PressureBar value={pressure != null ? Number(pressure) : null} />
                  </div>
                </button>
              );
            })
          )}
        </div>
      )}

      {activeTab === "complaints" && (
        <div className="space-y-2">
          {complaints.length === 0 ? (
            <EmptyState title="No complaints" message="No recent customer complaints for this DMA." />
          ) : (
            complaints.map((c: any, i: number) => (
              <div key={i} className="bg-gray-50 rounded-lg p-3 text-sm">
                <div className="flex justify-between items-start">
                  <span className="font-medium">{humanize(c.complaint_type) || "Complaint"}</span>
                  <span className="text-xs text-gray-400">
                    {c.reported_at ? new Date(c.reported_at).toLocaleString() : ""}
                  </span>
                </div>
                {c.description && (
                  <p className="text-gray-600 text-xs mt-1">{c.description}</p>
                )}
                {c.property_id && (
                  <p className="text-gray-400 text-xs mt-0.5">Property: {c.property_id}</p>
                )}
              </div>
            ))
          )}
        </div>
      )}

      {activeTab === "impact" && <CustomerImpact dmaCode={dmaCode} />}
    </div>
  );
}
