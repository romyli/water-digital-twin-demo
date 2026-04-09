import { GenieChat } from "@databricks/appkit-ui/react";

/**
 * Network Operations Genie Space — natural language interface for operators
 * to query real-time DMA health, sensor telemetry, pressure trends, and anomaly data.
 *
 * The "operator" alias maps to the Genie Space configured in the backend
 * via DATABRICKS_GENIE_SPACE_ID.
 */
export default function GeniePage() {
  return (
    <div className="flex flex-col h-[calc(100vh-3.5rem)]">
      <div className="border-b border-gray-200 bg-white px-6 py-3">
        <h1 className="text-lg font-bold text-water-800">Network Operations Assistant</h1>
        <p className="text-sm text-gray-500">
          Ask questions about DMA health, sensor telemetry, pressure trends, anomaly scores, and asset status.
        </p>
      </div>
      <div className="flex-1 min-h-0">
        <GenieChat alias="operator" />
      </div>
    </div>
  );
}
