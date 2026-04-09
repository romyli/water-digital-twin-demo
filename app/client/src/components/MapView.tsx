import { useEffect, useRef, useState, useCallback } from "react";
import maplibregl from "maplibre-gl";
import { useQuery } from "@tanstack/react-query";
import { fetchMapGeoJSON, fetchMapAssets } from "../api";
import DMADetail from "./DMADetail";
import LoadingSpinner from "./common/LoadingSpinner";

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

// Free OSM-based tile style (no API key required)
const MAP_STYLE = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json";

export default function MapView({ activeIncident }: { activeIncident: any }) {
  const mapContainer = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const popupRef = useRef<maplibregl.Popup | null>(null);
  const [selectedDMA, setSelectedDMA] = useState<string | null>(null);
  const [filter, setFilter] = useState("ALL");

  const { data: geojson, isLoading } = useQuery({
    queryKey: ["mapGeoJSON"],
    queryFn: fetchMapGeoJSON,
    staleTime: 60_000,
  });

  const { data: assetGeoJSON } = useQuery({
    queryKey: ["mapAssets", selectedDMA],
    queryFn: () => fetchMapAssets(selectedDMA!),
    enabled: !!selectedDMA,
  });

  const filterFeatures = useCallback(
    (fc: any) => {
      if (!fc?.features) return fc;
      if (filter === "ALL") return fc;
      return {
        ...fc,
        features: fc.features.filter((f: any) => {
          const rag = f.properties?.rag_status?.toUpperCase();
          if (filter === "ALARMED") return rag === "RED";
          if (filter === "CHANGED") return rag === "RED" || rag === "AMBER";
          if (filter === "SENSITIVE") return f.properties?.is_sensitive;
          return true;
        }),
      };
    },
    [filter]
  );

  // Initialize map
  useEffect(() => {
    if (mapRef.current || !mapContainer.current) return;
    const map = new maplibregl.Map({
      container: mapContainer.current,
      style: MAP_STYLE,
      center: [-0.08, 51.49],
      zoom: 11,
    });
    map.addControl(new maplibregl.NavigationControl(), "top-right");
    mapRef.current = map;
    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, []);

  // Load DMA polygons
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !geojson) return;

    const loadData = () => {
      const fc = filterFeatures(geojson);
      const fillExpr: any = [
        "match", ["get", "rag_status"],
        "RED", RAG_FILL.RED, "AMBER", RAG_FILL.AMBER, "GREEN", RAG_FILL.GREEN,
        RAG_FILL.GREEN,
      ];
      const strokeExpr: any = [
        "match", ["get", "rag_status"],
        "RED", RAG_STROKE.RED, "AMBER", RAG_STROKE.AMBER, "GREEN", RAG_STROKE.GREEN,
        RAG_STROKE.GREEN,
      ];

      if (map.getSource("dma-polygons")) {
        (map.getSource("dma-polygons") as maplibregl.GeoJSONSource).setData(fc);
      } else {
        map.addSource("dma-polygons", { type: "geojson", data: fc });
        map.addLayer({
          id: "dma-fill",
          type: "fill",
          source: "dma-polygons",
          paint: { "fill-color": fillExpr, "fill-opacity": 0.5 },
        });
        map.addLayer({
          id: "dma-outline",
          type: "line",
          source: "dma-polygons",
          paint: { "line-color": strokeExpr, "line-width": 2 },
        });

        map.on("click", "dma-fill", (e) => {
          const props = e.features?.[0]?.properties;
          if (props?.dma_code) setSelectedDMA(props.dma_code);
        });
        map.on("mouseenter", "dma-fill", () => {
          map.getCanvas().style.cursor = "pointer";
        });
        map.on("mouseleave", "dma-fill", () => {
          map.getCanvas().style.cursor = "";
          if (popupRef.current) {
            popupRef.current.remove();
            popupRef.current = null;
          }
        });
        map.on("mousemove", "dma-fill", (e) => {
          const props = e.features?.[0]?.properties;
          if (!props) return;
          const html = `
            <div class="text-xs">
              <div class="font-semibold">${props.dma_name || props.dma_code}</div>
              <div>Status: <strong>${props.rag_status || "GREEN"}</strong></div>
              ${props.anomaly_confidence ? `<div>Anomaly: ${(props.anomaly_confidence * 100).toFixed(0)}%</div>` : ""}
            </div>
          `;
          if (popupRef.current) {
            popupRef.current.setLngLat(e.lngLat).setHTML(html);
          } else {
            popupRef.current = new maplibregl.Popup({ closeButton: false, closeOnClick: false })
              .setLngLat(e.lngLat)
              .setHTML(html)
              .addTo(map);
          }
        });
      }
    };

    if (map.loaded()) loadData();
    else map.on("load", loadData);
  }, [geojson, filter, filterFeatures]);

  // Load asset overlays
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;

    const loadAssets = () => {
      if (map.getSource("dma-assets")) {
        (map.getSource("dma-assets") as maplibregl.GeoJSONSource).setData(
          assetGeoJSON || { type: "FeatureCollection", features: [] }
        );
        return;
      }
      if (!assetGeoJSON) return;
      map.addSource("dma-assets", { type: "geojson", data: assetGeoJSON });
      map.addLayer({
        id: "asset-lines",
        type: "line",
        source: "dma-assets",
        filter: ["==", ["geometry-type"], "LineString"],
        paint: { "line-color": "#6366F1", "line-width": 3, "line-dasharray": [2, 2] },
      });
      map.addLayer({
        id: "asset-points",
        type: "circle",
        source: "dma-assets",
        filter: ["==", ["geometry-type"], "Point"],
        paint: { "circle-radius": 6, "circle-color": "#6366F1", "circle-stroke-width": 2, "circle-stroke-color": "#fff" },
      });
    };

    if (map.loaded()) loadAssets();
    else map.on("load", loadAssets);
  }, [assetGeoJSON]);

  if (isLoading) return <LoadingSpinner message="Loading network map..." />;

  const filterButtons = [
    { key: "ALL", label: "All DMAs" },
    { key: "CHANGED", label: "Changed" },
    { key: "ALARMED", label: "Alarmed" },
    { key: "SENSITIVE", label: "Sensitive" },
  ];

  return (
    <div className="relative h-[calc(100vh-3.5rem)] flex">
      <div className="flex-1 relative">
        <div ref={mapContainer} className="absolute inset-0" />
        <div className="absolute top-3 left-3 z-10 flex gap-1.5">
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
