import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  fetchDMADetail,
  fetchDMASensors,
  fetchDMAComplaints,
  fetchDMAAssets,
  fetchDMARagHistory,
  fetchReservoirs,
} from "../api";
import RAGBadge from "./common/RAGBadge";
import TimelineStrip from "./common/TimelineStrip";
import LoadingSpinner from "./common/LoadingSpinner";
import EmptyState from "./common/EmptyState";
import AssetDetail from "./AssetDetail";
import CustomerImpact from "./CustomerImpact";

export default function DMADetail({
  dmaCode,
  activeIncident,
}: {
  dmaCode: string;
  activeIncident: any;
}) {
  const [activeTab, setActiveTab] = useState("summary");
  const [selectedSensor, setSelectedSensor] = useState<string | null>(null);

  const { data: detail, isLoading: loadingDetail } = useQuery({
    queryKey: ["dmaDetail", dmaCode],
    queryFn: () => fetchDMADetail(dmaCode),
    enabled: !!dmaCode,
  });

  const { data: sensorsData } = useQuery({
    queryKey: ["dmaSensors", dmaCode],
    queryFn: () => fetchDMASensors(dmaCode),
    enabled: !!dmaCode,
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

  const pumpAssets = assets.filter((a: any) => a.asset_type === "pump_station");
  const trunkMains = assets.filter((a: any) => a.asset_type === "trunk_main");
  const rootCauseText =
    pumpAssets.length > 0 && trunkMains.length > 0
      ? `Root Cause: Pump station ${pumpAssets[0].asset_id} tripped at ${
          pumpAssets[0].last_event_time || "02:03"
        } \u2192 Trunk Main ${trunkMains[0].asset_id} \u2192 ${dmaCode}. Downstream: ${
          detail?.downstream_dma_count || 3
        } DMAs, ~${detail?.total_properties || "1,400"} properties`
      : null;

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
        <AssetDetail sensorId={selectedSensor} dmaCode={dmaCode} activeIncident={activeIncident} />
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

      <div className="flex gap-1 border-b border-gray-100 pb-1">
        {tabs.map((t) => (
          <button
            key={t.key}
            onClick={() => setActiveTab(t.key)}
            className={`px-3 py-1.5 text-xs font-medium rounded-t-lg transition-colors ${
              activeTab === t.key
                ? "bg-water-50 text-water-700 border-b-2 border-water-600"
                : "text-gray-500 hover:text-gray-700"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      {activeTab === "summary" && (
        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-3 text-sm">
            <div className="bg-gray-50 rounded-lg p-3">
              <p className="text-gray-500 text-xs">Avg Pressure</p>
              <p className="font-semibold text-lg">
                {detail?.avg_pressure != null ? `${Number(detail.avg_pressure).toFixed(1)} m` : "\u2014"}
              </p>
            </div>
            <div className="bg-gray-50 rounded-lg p-3">
              <p className="text-gray-500 text-xs">Avg Flow</p>
              <p className="font-semibold text-lg">
                {detail?.avg_flow != null ? `${Number(detail.avg_flow).toFixed(1)} l/s` : "\u2014"}
              </p>
            </div>
            <div className="bg-gray-50 rounded-lg p-3">
              <p className="text-gray-500 text-xs">Sensors</p>
              <p className="font-semibold text-lg">{detail?.sensor_count ?? sensors.length}</p>
            </div>
            <div className="bg-gray-50 rounded-lg p-3">
              <p className="text-gray-500 text-xs">Properties</p>
              <p className="font-semibold text-lg">{detail?.property_count ?? "\u2014"}</p>
            </div>
          </div>

          {rootCauseText && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm text-red-800">
              <p className="font-medium mb-1">Upstream Root Cause</p>
              <p>{rootCauseText}</p>
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
                      label={`${r.level_pct ?? "\u2014"}%`}
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
                      <span className="text-gray-500 ml-2 text-xs">{a.asset_type}</span>
                    </div>
                    <RAGBadge status={a.rag_status || "GREEN"} />
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {activeTab === "sensors" && (
        <div className="space-y-1.5">
          {sortedSensors.length === 0 ? (
            <EmptyState title="No sensors" message="No sensors found in this DMA." />
          ) : (
            sortedSensors.map((s: any) => {
              const pressure = s.latest_pressure ?? s.avg_pressure;
              const isLow = pressure != null && pressure < 10;
              return (
                <button
                  key={s.sensor_id}
                  onClick={() => setSelectedSensor(s.sensor_id)}
                  className="w-full text-left bg-gray-50 hover:bg-gray-100 rounded-lg px-3 py-2.5 text-sm transition-colors"
                >
                  <div className="flex items-center justify-between">
                    <span className="font-medium">{s.sensor_id}</span>
                    <RAGBadge
                      status={isLow ? "RED" : "GREEN"}
                      label={pressure != null ? `${Number(pressure).toFixed(1)} m` : "\u2014"}
                    />
                  </div>
                  {s.sensor_type && <p className="text-xs text-gray-500 mt-0.5">{s.sensor_type}</p>}
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
                  <span className="font-medium">{c.complaint_type || "Complaint"}</span>
                  <span className="text-xs text-gray-400">
                    {c.reported_at ? new Date(c.reported_at).toLocaleString() : "\u2014"}
                  </span>
                </div>
                <p className="text-gray-600 text-xs mt-1">{c.description || "No description"}</p>
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
