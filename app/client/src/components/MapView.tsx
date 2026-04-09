import { useState, useCallback, useMemo } from "react";
import { Map, Source, Layer, Popup, NavigationControl } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import { useQuery } from "@tanstack/react-query";
import {
  fetchMapGeoJSON,
  fetchMapAssets,
  fetchMapSensors,
  fetchMapComplaints,
  fetchMapSensitive,
} from "../api";
import DMADetail from "./DMADetail";
import LoadingSpinner from "./common/LoadingSpinner";
import type { MapLayerMouseEvent } from "react-map-gl/maplibre";

const MAP_STYLE = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json";

const RAG_FILL: Record<string, string> = {
  RED: "rgba(220, 38, 38, 0.35)",
  AMBER: "rgba(245, 158, 11, 0.3)",
  GREEN: "rgba(22, 163, 74, 0.2)",
};
const RAG_STROKE: Record<string, string> = {
  RED: "#DC2626",
  AMBER: "#F59E0B",
  GREEN: "#16A34A",
};

const fillColor: any = [
  "match", ["get", "rag_status"],
  "RED", RAG_FILL.RED, "AMBER", RAG_FILL.AMBER, "GREEN", RAG_FILL.GREEN,
  RAG_FILL.GREEN,
];
const strokeColor: any = [
  "match", ["get", "rag_status"],
  "RED", RAG_STROKE.RED, "AMBER", RAG_STROKE.AMBER, "GREEN", RAG_STROKE.GREEN,
  RAG_STROKE.GREEN,
];

// Pressure-based color for sensor dots
const sensorColor: any = [
  "case",
  ["==", ["get", "rag_status"], "RED"], "#DC2626",
  ["==", ["get", "rag_status"], "AMBER"], "#F59E0B",
  "#16A34A",
];

// Sensitive premise type → color
const sensitiveColor: any = [
  "match", ["get", "sensitive_type"],
  "hospital", "#DC2626",
  "school", "#2563EB",
  "care_home", "#7C3AED",
  "dialysis_centre", "#EA580C",
  "#6366F1",
];

const SENSITIVE_LABELS: Record<string, string> = {
  hospital: "Hospital",
  school: "School",
  care_home: "Care Home",
  dialysis_centre: "Dialysis",
};

export default function MapView({ activeIncident }: { activeIncident: any }) {
  const [selectedDMA, setSelectedDMA] = useState<string | null>(null);
  const [filter, setFilter] = useState("ALL");
  const [showSensors, setShowSensors] = useState(false);
  const [showOverlays, setShowOverlays] = useState(true);
  const [hoverInfo, setHoverInfo] = useState<{
    lng: number; lat: number;
    dma_code: string; dma_name: string; rag_status: string;
  } | null>(null);

  // --- Data queries ---
  const { data: geojson, isLoading, error: geoError } = useQuery({
    queryKey: ["mapGeoJSON"],
    queryFn: fetchMapGeoJSON,
    staleTime: 60_000,
  });

  const { data: assetGeoJSON } = useQuery({
    queryKey: ["mapAssets", selectedDMA],
    queryFn: () => fetchMapAssets(selectedDMA!),
    enabled: !!selectedDMA,
  });

  const { data: sensorGeoJSON } = useQuery({
    queryKey: ["mapSensors"],
    queryFn: fetchMapSensors,
    enabled: showSensors,
    staleTime: 60_000,
  });

  const { data: complaintGeoJSON } = useQuery({
    queryKey: ["mapComplaints", selectedDMA],
    queryFn: () => fetchMapComplaints(selectedDMA!),
    enabled: !!selectedDMA && showOverlays,
  });

  const { data: sensitiveGeoJSON } = useQuery({
    queryKey: ["mapSensitive", selectedDMA],
    queryFn: () => fetchMapSensitive(selectedDMA!),
    enabled: !!selectedDMA && showOverlays,
  });

  // --- Filtering ---
  const filteredGeoJSON = useMemo(() => {
    if (!geojson?.features) return geojson;
    if (filter === "ALL") return geojson;
    return {
      ...geojson,
      features: geojson.features.filter((f: any) => {
        const rag = f.properties?.rag_status?.toUpperCase();
        if (filter === "ALARMED") return rag === "RED";
        if (filter === "CHANGED") return rag === "RED" || rag === "AMBER";
        if (filter === "SENSITIVE") return f.properties?.is_sensitive;
        return true;
      }),
    };
  }, [geojson, filter]);

  // --- Interactions ---
  const onClick = useCallback((e: MapLayerMouseEvent) => {
    const feature = e.features?.[0];
    if (feature?.properties?.dma_code) {
      setSelectedDMA(feature.properties.dma_code);
    }
  }, []);

  const onHover = useCallback((e: MapLayerMouseEvent) => {
    const feature = e.features?.[0];
    if (feature?.properties) {
      setHoverInfo({
        lng: e.lngLat.lng,
        lat: e.lngLat.lat,
        dma_code: feature.properties.dma_code,
        dma_name: feature.properties.dma_name || feature.properties.dma_code,
        rag_status: feature.properties.rag_status || "GREEN",
      });
    } else {
      setHoverInfo(null);
    }
  }, []);

  if (isLoading) return <LoadingSpinner message="Loading network map..." />;

  const geoErrorMsg = geoError
    ? `Failed to load DMA data: ${(geoError as Error).message}.`
    : null;

  const complaintCount = complaintGeoJSON?.features?.length || 0;
  const sensitiveCount = sensitiveGeoJSON?.features?.length || 0;

  const filterButtons = [
    { key: "ALL", label: "All DMAs" },
    { key: "CHANGED", label: "Changed" },
    { key: "ALARMED", label: "Alarmed" },
    { key: "SENSITIVE", label: "Sensitive" },
  ];

  return (
    <div className="relative h-[calc(100vh-3.5rem)] flex">
      <div className="flex-1 relative">
        <Map
          initialViewState={{ longitude: -0.08, latitude: 51.49, zoom: 11 }}
          style={{ position: "absolute", inset: 0 }}
          mapStyle={MAP_STYLE}
          interactiveLayerIds={["dma-fill"]}
          onClick={onClick}
          onMouseMove={onHover}
          onMouseLeave={() => setHoverInfo(null)}
          cursor={hoverInfo ? "pointer" : "grab"}
        >
          <NavigationControl position="top-right" />

          {/* DMA polygons */}
          {filteredGeoJSON && (
            <Source id="dma-polygons" type="geojson" data={filteredGeoJSON}>
              <Layer
                id="dma-fill"
                type="fill"
                paint={{ "fill-color": fillColor, "fill-opacity": 0.5 }}
              />
              <Layer
                id="dma-outline"
                type="line"
                paint={{ "line-color": strokeColor, "line-width": 2 }}
              />
            </Source>
          )}

          {/* Sensor pressure overlay */}
          {showSensors && sensorGeoJSON && (
            <Source id="sensor-points" type="geojson" data={sensorGeoJSON}>
              <Layer
                id="sensor-dots"
                type="circle"
                paint={{
                  "circle-radius": ["interpolate", ["linear"], ["zoom"], 10, 2, 14, 5],
                  "circle-color": sensorColor,
                  "circle-opacity": 0.8,
                  "circle-stroke-width": 0.5,
                  "circle-stroke-color": "#fff",
                }}
              />
            </Source>
          )}

          {/* Infrastructure assets for selected DMA */}
          {assetGeoJSON && (
            <Source id="dma-assets" type="geojson" data={assetGeoJSON}>
              <Layer
                id="asset-lines"
                type="line"
                filter={["==", ["geometry-type"], "LineString"]}
                paint={{ "line-color": "#6366F1", "line-width": 3, "line-dasharray": [2, 2] }}
              />
              <Layer
                id="asset-points"
                type="circle"
                filter={["==", ["geometry-type"], "Point"]}
                paint={{
                  "circle-radius": 7,
                  "circle-color": "#6366F1",
                  "circle-stroke-width": 2,
                  "circle-stroke-color": "#fff",
                }}
              />
            </Source>
          )}

          {/* Customer complaint markers (pulsing) */}
          {showOverlays && complaintGeoJSON && complaintGeoJSON.features?.length > 0 && (
            <Source id="complaint-points" type="geojson" data={complaintGeoJSON}>
              <Layer
                id="complaint-pulse"
                type="circle"
                paint={{
                  "circle-radius": 12,
                  "circle-color": "rgba(220, 38, 38, 0.15)",
                  "circle-stroke-width": 0,
                }}
              />
              <Layer
                id="complaint-dots"
                type="circle"
                paint={{
                  "circle-radius": 5,
                  "circle-color": "#DC2626",
                  "circle-stroke-width": 2,
                  "circle-stroke-color": "#fff",
                }}
              />
            </Source>
          )}

          {/* Sensitive premises markers */}
          {showOverlays && sensitiveGeoJSON && sensitiveGeoJSON.features?.length > 0 && (
            <Source id="sensitive-points" type="geojson" data={sensitiveGeoJSON}>
              <Layer
                id="sensitive-dots"
                type="circle"
                paint={{
                  "circle-radius": 7,
                  "circle-color": sensitiveColor,
                  "circle-stroke-width": 2,
                  "circle-stroke-color": "#fff",
                }}
              />
              <Layer
                id="sensitive-labels"
                type="symbol"
                layout={{
                  "text-field": ["get", "sensitive_type"],
                  "text-size": 9,
                  "text-offset": [0, 1.5],
                  "text-anchor": "top",
                }}
                paint={{
                  "text-color": "#4B5563",
                  "text-halo-color": "#fff",
                  "text-halo-width": 1,
                }}
              />
            </Source>
          )}

          {hoverInfo && (
            <Popup
              longitude={hoverInfo.lng}
              latitude={hoverInfo.lat}
              closeButton={false}
              closeOnClick={false}
              anchor="bottom"
              offset={10}
            >
              <div className="text-xs">
                <div className="font-semibold">{hoverInfo.dma_name}</div>
                <div>Status: <strong>{hoverInfo.rag_status}</strong></div>
              </div>
            </Popup>
          )}
        </Map>

        {/* Error banner */}
        {geoErrorMsg && (
          <div className="absolute top-3 left-3 right-3 z-20 bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-sm text-red-800">
            {geoErrorMsg}
          </div>
        )}

        {/* Filter bar */}
        <div className={`absolute ${geoErrorMsg ? "top-16" : "top-3"} left-3 z-10 flex gap-1.5`}>
          {filterButtons.map((b) => (
            <button
              key={b.key}
              onClick={() => setFilter(b.key)}
              className={`px-3 py-1.5 rounded-lg text-xs font-medium shadow-sm transition-colors ${
                filter === b.key
                  ? "bg-water-700 text-white"
                  : "bg-white text-gray-700 hover:bg-gray-50 border border-gray-200"
              }`}
            >
              {b.label}
            </button>
          ))}
          <div className="w-px bg-gray-300" />
          <button
            onClick={() => setShowSensors((v) => !v)}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium shadow-sm transition-colors ${
              showSensors
                ? "bg-emerald-600 text-white"
                : "bg-white text-gray-700 hover:bg-gray-50 border border-gray-200"
            }`}
          >
            Sensors
          </button>
          <button
            onClick={() => setShowOverlays((v) => !v)}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium shadow-sm transition-colors ${
              showOverlays
                ? "bg-red-600 text-white"
                : "bg-white text-gray-700 hover:bg-gray-50 border border-gray-200"
            }`}
          >
            Impact
          </button>
        </div>

        {/* Legend (when overlays visible on selected DMA) */}
        {selectedDMA && showOverlays && (complaintCount > 0 || sensitiveCount > 0) && (
          <div className="absolute bottom-4 left-3 z-10 bg-white/95 rounded-lg shadow-sm border border-gray-200 px-3 py-2 text-xs space-y-1.5">
            {complaintCount > 0 && (
              <div className="flex items-center gap-2">
                <span className="w-3 h-3 rounded-full bg-red-600 inline-block" />
                <span className="text-gray-700">Complaints ({complaintCount})</span>
              </div>
            )}
            {sensitiveCount > 0 && (
              <>
                {Object.entries(SENSITIVE_LABELS).map(([key, label]) => (
                  <div key={key} className="flex items-center gap-2">
                    <span
                      className="w-3 h-3 rounded-full inline-block"
                      style={{
                        backgroundColor:
                          key === "hospital" ? "#DC2626"
                          : key === "school" ? "#2563EB"
                          : key === "care_home" ? "#7C3AED"
                          : "#EA580C",
                      }}
                    />
                    <span className="text-gray-700">{label}</span>
                  </div>
                ))}
              </>
            )}
          </div>
        )}
      </div>

      {/* Side panel */}
      {selectedDMA && (
        <div className="w-[420px] border-l border-gray-200 bg-white overflow-y-auto">
          <div className="p-4 border-b border-gray-100 flex items-center justify-between">
            <h2 className="font-semibold text-water-800">{selectedDMA}</h2>
            <button
              onClick={() => setSelectedDMA(null)}
              className="text-gray-400 hover:text-gray-600 text-lg leading-none"
            >
              &times;
            </button>
          </div>
          <DMADetail dmaCode={selectedDMA} activeIncident={activeIncident} />
        </div>
      )}
    </div>
  );
}
