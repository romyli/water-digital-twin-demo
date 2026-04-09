import { useState, useCallback, useMemo, useEffect } from "react";
import { Map, Source, Layer, Popup, NavigationControl } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import { useQuery } from "@tanstack/react-query";
import {
  fetchMapGeoJSON,
  fetchMapAssets,
  fetchMapSensors,
  fetchMapAllComplaints,
  fetchMapAllSensitive,
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

const sensorColor: any = [
  "interpolate", ["linear"],
  ["coalesce", ["get", "avg_pressure"], 25],
  0, "#DC2626",
  10, "#F59E0B",
  20, "#16A34A",
  30, "#059669",
];

const SENSITIVE_CONFIG: Record<string, { color: string; label: string; letter: string }> = {
  hospital:         { color: "#DC2626", label: "Hospital",  letter: "H" },
  school:           { color: "#2563EB", label: "School",    letter: "S" },
  care_home:        { color: "#7C3AED", label: "Care Home", letter: "C" },
  dialysis_centre:  { color: "#EA580C", label: "Dialysis",  letter: "D" },
};

const sensitiveTextColor: any = [
  "match", ["get", "sensitive_type"],
  "hospital", "#DC2626",
  "school", "#2563EB",
  "care_home", "#7C3AED",
  "dialysis_centre", "#EA580C",
  "#6366F1",
];

const sensitiveLetter: any = [
  "match", ["get", "sensitive_type"],
  "hospital", "H",
  "school", "S",
  "care_home", "C",
  "dialysis_centre", "D",
  "?",
];

export default function MapView({ activeIncident }: { activeIncident: any }) {
  const [selectedDMA, setSelectedDMA] = useState<string | null>(null);
  const [filter, setFilter] = useState("ALL");
  const [showSensors, setShowSensors] = useState(false);
  const [showOverlays, setShowOverlays] = useState(true);
  const [hoverInfo, setHoverInfo] = useState<{
    lng: number; lat: number;
    dma_code: string; dma_name: string; rag_status: string;
  } | null>(null);
  const [overlayHover, setOverlayHover] = useState<{
    lng: number; lat: number;
    layer: "sensor" | "complaint" | "sensitive";
    props: Record<string, any>;
  } | null>(null);

  // Escape to close panel
  useEffect(() => {
    const handler = () => setSelectedDMA(null);
    window.addEventListener("app:escape", handler);
    return () => window.removeEventListener("app:escape", handler);
  }, []);

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
    queryKey: ["mapAllComplaints"],
    queryFn: fetchMapAllComplaints,
    enabled: showOverlays,
    staleTime: 60_000,
  });

  const { data: sensitiveGeoJSON } = useQuery({
    queryKey: ["mapAllSensitive"],
    queryFn: fetchMapAllSensitive,
    enabled: showOverlays,
    staleTime: 60_000,
  });

  // Count DMAs by RAG status
  const dmaCounts = useMemo(() => {
    const c = { ALL: 0, ALARMED: 0, CHANGED: 0, SENSITIVE: 0 };
    if (!geojson?.features) return c;
    c.ALL = geojson.features.length;
    geojson.features.forEach((f: any) => {
      const rag = f.properties?.rag_status?.toUpperCase();
      if (rag === "RED") { c.ALARMED++; c.CHANGED++; }
      else if (rag === "AMBER") { c.CHANGED++; }
      // Check both is_sensitive flag and sensitive_premises_count > 0
      if (f.properties?.is_sensitive || (f.properties?.sensitive_premises_count > 0)) c.SENSITIVE++;
    });
    return c;
  }, [geojson]);

  const filteredGeoJSON = useMemo(() => {
    if (!geojson?.features) return geojson;
    if (filter === "ALL") return geojson;
    return {
      ...geojson,
      features: geojson.features.filter((f: any) => {
        const rag = f.properties?.rag_status?.toUpperCase();
        if (filter === "ALARMED") return rag === "RED";
        if (filter === "CHANGED") return rag === "RED" || rag === "AMBER";
        if (filter === "SENSITIVE") return f.properties?.is_sensitive || (f.properties?.sensitive_premises_count > 0);
        return true;
      }),
    };
  }, [geojson, filter]);

  const overlayLayerIds = ["sensor-dots", "complaint-dots", "sensitive-bg"];
  const allInteractiveIds = ["dma-fill", ...overlayLayerIds];

  const onClick = useCallback((e: MapLayerMouseEvent) => {
    const feature = e.features?.[0];
    if (feature?.layer?.id === "dma-fill" && feature?.properties?.dma_code) {
      setSelectedDMA(feature.properties.dma_code);
    }
  }, []);

  const onHover = useCallback((e: MapLayerMouseEvent) => {
    const feature = e.features?.[0];
    if (!feature?.properties) {
      setHoverInfo(null);
      setOverlayHover(null);
      return;
    }
    const layerId = feature.layer?.id;
    if (layerId === "sensor-dots") {
      setOverlayHover({ lng: e.lngLat.lng, lat: e.lngLat.lat, layer: "sensor", props: feature.properties });
      setHoverInfo(null);
    } else if (layerId === "complaint-dots") {
      setOverlayHover({ lng: e.lngLat.lng, lat: e.lngLat.lat, layer: "complaint", props: feature.properties });
      setHoverInfo(null);
    } else if (layerId === "sensitive-bg") {
      setOverlayHover({ lng: e.lngLat.lng, lat: e.lngLat.lat, layer: "sensitive", props: feature.properties });
      setHoverInfo(null);
    } else if (layerId === "dma-fill") {
      setHoverInfo({
        lng: e.lngLat.lng,
        lat: e.lngLat.lat,
        dma_code: feature.properties.dma_code,
        dma_name: feature.properties.dma_name || feature.properties.dma_code,
        rag_status: feature.properties.rag_status || "GREEN",
      });
      setOverlayHover(null);
    } else {
      setHoverInfo(null);
      setOverlayHover(null);
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
    <div className="relative h-full flex">
      <div className="flex-1 relative">
        <Map
          initialViewState={{ longitude: -0.08, latitude: 51.49, zoom: 11 }}
          style={{ position: "absolute", inset: 0 }}
          mapStyle={MAP_STYLE}
          interactiveLayerIds={allInteractiveIds}
          onClick={onClick}
          onMouseMove={onHover}
          onMouseLeave={() => { setHoverInfo(null); setOverlayHover(null); }}
          cursor={(hoverInfo || overlayHover) ? "pointer" : "grab"}
        >
          <NavigationControl position="top-right" />

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

          {showOverlays && complaintGeoJSON && complaintGeoJSON.features?.length > 0 && (
            <Source id="complaint-points" type="geojson" data={complaintGeoJSON}>
              <Layer
                id="complaint-ring"
                type="circle"
                paint={{
                  "circle-radius": 16,
                  "circle-color": "transparent",
                  "circle-stroke-width": 2,
                  "circle-stroke-color": "rgba(220, 38, 38, 0.4)",
                }}
              />
              <Layer
                id="complaint-dots"
                type="symbol"
                layout={{
                  "icon-image": "diamond",
                  "icon-size": 0.8,
                  "icon-allow-overlap": true,
                  "text-field": "!",
                  "text-size": 11,
                  "text-font": ["Open Sans Bold"],
                  "text-allow-overlap": true,
                }}
                paint={{
                  "text-color": "#fff",
                  "text-halo-color": "#DC2626",
                  "text-halo-width": 8,
                }}
              />
            </Source>
          )}

          {showOverlays && sensitiveGeoJSON && sensitiveGeoJSON.features?.length > 0 && (
            <Source id="sensitive-points" type="geojson" data={sensitiveGeoJSON}>
              <Layer
                id="sensitive-bg"
                type="circle"
                paint={{
                  "circle-radius": 11,
                  "circle-color": "#fff",
                  "circle-stroke-width": 2.5,
                  "circle-stroke-color": sensitiveTextColor,
                }}
              />
              <Layer
                id="sensitive-letter"
                type="symbol"
                layout={{
                  "text-field": sensitiveLetter,
                  "text-size": 12,
                  "text-font": ["Open Sans Bold"],
                  "text-allow-overlap": true,
                }}
                paint={{
                  "text-color": sensitiveTextColor,
                }}
              />
            </Source>
          )}

          {/* DMA hover tooltip — dark style */}
          {hoverInfo && (
            <Popup
              longitude={hoverInfo.lng}
              latitude={hoverInfo.lat}
              closeButton={false}
              closeOnClick={false}
              anchor="bottom"
              offset={10}
              className="map-tooltip-dark"
            >
              <div className="bg-gray-900 text-white rounded-lg px-3 py-2 text-xs shadow-xl border border-gray-700">
                <div className="font-semibold">{hoverInfo.dma_name}</div>
                <div>Status: <strong>{hoverInfo.rag_status}</strong></div>
              </div>
            </Popup>
          )}

          {overlayHover && (
            <Popup
              longitude={overlayHover.lng}
              latitude={overlayHover.lat}
              closeButton={false}
              closeOnClick={false}
              anchor="bottom"
              offset={14}
              className="map-tooltip-dark"
            >
              <div className="bg-gray-900 text-white rounded-lg px-3 py-2 text-xs shadow-xl border border-gray-700">
                {overlayHover.layer === "sensor" && (
                  <div className="space-y-0.5">
                    <div className="font-semibold">{overlayHover.props.sensor_id}</div>
                    <div>Type: {overlayHover.props.sensor_type}</div>
                    <div>DMA: {overlayHover.props.dma_code}</div>
                    {overlayHover.props.avg_pressure != null && (
                      <div>Pressure: <strong>{Number(overlayHover.props.avg_pressure).toFixed(1)}m</strong></div>
                    )}
                  </div>
                )}
                {overlayHover.layer === "complaint" && (
                  <div className="space-y-0.5">
                    <div className="font-semibold text-red-300">Customer Complaint</div>
                    <div>Type: {overlayHover.props.complaint_type}</div>
                    <div>Property: {overlayHover.props.property_id}</div>
                    {overlayHover.props.complaint_timestamp && (
                      <div>Reported: {new Date(overlayHover.props.complaint_timestamp).toLocaleString()}</div>
                    )}
                  </div>
                )}
                {overlayHover.layer === "sensitive" && (
                  <div className="space-y-0.5">
                    <div className="font-semibold" style={{ color: SENSITIVE_CONFIG[overlayHover.props.sensitive_type]?.color }}>
                      {SENSITIVE_CONFIG[overlayHover.props.sensitive_type]?.label || overlayHover.props.sensitive_type}
                    </div>
                    <div>{overlayHover.props.property_id}</div>
                    {overlayHover.props.height != null && (
                      <div>Height: {overlayHover.props.height}m</div>
                    )}
                  </div>
                )}
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

        {/* Bottom-center translucent filter bar */}
        <div className="absolute bottom-6 left-1/2 -translate-x-1/2 z-10 flex items-center gap-1.5 bg-white/90 backdrop-blur-sm rounded-full shadow-lg px-2 py-1.5">
          {filterButtons.map((b) => (
            <button
              key={b.key}
              onClick={() => setFilter(b.key)}
              className={`px-3 py-1.5 rounded-full text-xs font-medium transition-colors ${
                filter === b.key
                  ? "bg-water-700 text-white"
                  : "text-gray-700 hover:bg-gray-100"
              }`}
            >
              {b.label} ({dmaCounts[b.key as keyof typeof dmaCounts]})
            </button>
          ))}
          <div className="w-px h-5 bg-gray-300" />
          <button
            onClick={() => setShowSensors((v) => !v)}
            className={`px-3 py-1.5 rounded-full text-xs font-medium transition-colors ${
              showSensors
                ? "bg-emerald-600 text-white"
                : "text-gray-700 hover:bg-gray-100"
            }`}
          >
            Sensors
          </button>
          <button
            onClick={() => setShowOverlays((v) => !v)}
            className={`px-3 py-1.5 rounded-full text-xs font-medium transition-colors ${
              showOverlays
                ? "bg-red-600 text-white"
                : "text-gray-700 hover:bg-gray-100"
            }`}
          >
            Complaints & Sensitive
          </button>
        </div>

        {/* Bottom-right RAG legend */}
        <div className="absolute bottom-20 right-3 z-10 bg-white/95 backdrop-blur-sm rounded-lg shadow-md border border-gray-200 px-4 py-3 text-sm space-y-2">
          <p className="text-xs font-semibold text-gray-500 uppercase tracking-wide">DMA Status</p>
          <div className="flex items-center gap-2.5">
            <span className="w-4 h-4 rounded" style={{ backgroundColor: "rgba(220, 38, 38, 0.7)" }} />
            <span className="text-gray-800 font-medium">RED — Alarmed</span>
          </div>
          <div className="flex items-center gap-2.5">
            <span className="w-4 h-4 rounded" style={{ backgroundColor: "rgba(245, 158, 11, 0.7)" }} />
            <span className="text-gray-800 font-medium">AMBER — Changed</span>
          </div>
          <div className="flex items-center gap-2.5">
            <span className="w-4 h-4 rounded" style={{ backgroundColor: "rgba(22, 163, 74, 0.5)" }} />
            <span className="text-gray-800 font-medium">GREEN — Normal</span>
          </div>
          {/* Sensitive + complaint legend when panel open */}
          {showOverlays && (complaintCount > 0 || sensitiveCount > 0) && (
            <>
              <div className="border-t border-gray-200 my-1" />
              {complaintCount > 0 && (
                <div className="flex items-center gap-2">
                  <span className="w-4 h-4 rounded-full bg-red-600 text-white text-[9px] font-bold flex items-center justify-center">!</span>
                  <span className="text-gray-700">Complaints ({complaintCount})</span>
                </div>
              )}
              {sensitiveCount > 0 && Object.entries(SENSITIVE_CONFIG).map(([key, cfg]) => (
                <div key={key} className="flex items-center gap-2">
                  <span
                    className="w-4 h-4 rounded-full bg-white border-2 text-[9px] font-bold flex items-center justify-center"
                    style={{ borderColor: cfg.color, color: cfg.color }}
                  >
                    {cfg.letter}
                  </span>
                  <span className="text-gray-700">{cfg.label}</span>
                </div>
              ))}
            </>
          )}
        </div>
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
