import { useState, useCallback, useMemo } from "react";
import { Map, Source, Layer, Popup, NavigationControl } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import { useQuery } from "@tanstack/react-query";
import { fetchMapGeoJSON, fetchMapAssets } from "../api";
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

export default function MapView({ activeIncident }: { activeIncident: any }) {
  const [selectedDMA, setSelectedDMA] = useState<string | null>(null);
  const [filter, setFilter] = useState("ALL");
  const [hoverInfo, setHoverInfo] = useState<{ lng: number; lat: number; dma_code: string; dma_name: string; rag_status: string } | null>(null);

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
                  "circle-radius": 6,
                  "circle-color": "#6366F1",
                  "circle-stroke-width": 2,
                  "circle-stroke-color": "#fff",
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

        {geoErrorMsg && (
          <div className="absolute top-3 left-3 right-3 z-20 bg-red-50 border border-red-200 rounded-lg px-4 py-3 text-sm text-red-800">
            {geoErrorMsg}
          </div>
        )}
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
        </div>
      </div>

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
