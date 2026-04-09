import { useState, useMemo } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  fetchSensorTelemetry,
  fetchSensorAnomalies,
  fetchDMARagHistory,
  fetchPlaybook,
  savePlaybookActions,
} from "../api";
import {
  ResponsiveContainer,
  ComposedChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  CartesianGrid,
} from "recharts";
import { format } from "date-fns";
import RAGBadge from "./common/RAGBadge";
import TimelineStrip from "./common/TimelineStrip";
import LoadingSpinner from "./common/LoadingSpinner";
import EmptyState from "./common/EmptyState";

export default function AssetDetail({
  sensorId,
  dmaCode,
  activeIncident,
}: {
  sensorId: string;
  dmaCode: string;
  activeIncident: any;
}) {
  const queryClient = useQueryClient();
  const [decisions, setDecisions] = useState<Record<string, string>>({});

  const { data: telemetryData, isLoading: loadingTelemetry } = useQuery({
    queryKey: ["telemetry", sensorId],
    queryFn: () => fetchSensorTelemetry(sensorId),
    enabled: !!sensorId,
  });

  const { data: anomalyData } = useQuery({
    queryKey: ["anomalies", sensorId],
    queryFn: () => fetchSensorAnomalies(sensorId),
    enabled: !!sensorId,
  });

  const { data: ragData } = useQuery({
    queryKey: ["dmaRag", dmaCode],
    queryFn: () => fetchDMARagHistory(dmaCode),
    enabled: !!dmaCode,
  });

  const incidentType = activeIncident?.incident_type || "low_pressure";
  const { data: playbookData } = useQuery({
    queryKey: ["playbook", incidentType],
    queryFn: () => fetchPlaybook(incidentType),
    enabled: !!incidentType,
  });

  const telemetry = telemetryData?.telemetry || [];
  const anomalies = anomalyData?.anomalies || [];
  const ragHistory = ragData?.rag_history || [];
  const playbookSteps = playbookData?.steps || [];

  const chartData = useMemo(() => {
    return telemetry.map((t: any) => ({
      time: t.ts ? format(new Date(t.ts), "HH:mm") : "",
      pressure: t.pressure != null ? Number(t.pressure) : null,
      flow: t.flow != null ? Number(t.flow) : null,
    }));
  }, [telemetry]);

  const firstAnomaly = anomalies.length > 0 ? anomalies[anomalies.length - 1] : null;
  const firstComplaintTime = activeIncident?.first_complaint_time;
  const anomalyLeadMin = useMemo(() => {
    if (!firstAnomaly?.scored_at || !firstComplaintTime) return null;
    const diff =
      (new Date(firstComplaintTime).getTime() - new Date(firstAnomaly.scored_at).getTime()) / 60000;
    return diff > 0 ? Math.round(diff) : null;
  }, [firstAnomaly, firstComplaintTime]);

  const saveMutation = useMutation({
    mutationFn: (actions: any[]) => savePlaybookActions(activeIncident?.incident_id, actions),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["playbook"] }),
  });

  const handleDecision = (actionId: string, decision: string) => {
    setDecisions((prev) => ({ ...prev, [actionId]: decision }));
  };

  const handleSavePlaybook = () => {
    const actions = Object.entries(decisions).map(([action_id, decision]) => ({
      action_id,
      decision,
    }));
    if (actions.length > 0) saveMutation.mutate(actions);
  };

  if (loadingTelemetry) return <LoadingSpinner message="Loading sensor data..." />;

  return (
    <div className="space-y-4">
      <h3 className="font-semibold text-water-800">{sensorId}</h3>

      {firstAnomaly && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm">
          <div className="flex items-center gap-2">
            <RAGBadge status="RED" label="Anomaly Detected" />
          </div>
          <p className="text-red-700 mt-1">
            Anomaly detected at{" "}
            {firstAnomaly.scored_at ? format(new Date(firstAnomaly.scored_at), "HH:mm") : "\u2014"}
            {anomalyLeadMin != null && (
              <span className="font-medium"> — {anomalyLeadMin} min before first complaint</span>
            )}
          </p>
          {firstAnomaly.score != null && (
            <p className="text-red-600 text-xs mt-0.5">
              Confidence: {(Number(firstAnomaly.score) * 100).toFixed(0)}%
            </p>
          )}
        </div>
      )}

      {chartData.length > 0 ? (
        <div className="bg-gray-50 rounded-lg p-3">
          <p className="text-xs font-medium text-gray-500 mb-2">Telemetry (24h)</p>
          <ResponsiveContainer width="100%" height={220}>
            <ComposedChart data={chartData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="time" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
              <YAxis
                yAxisId="left"
                tick={{ fontSize: 10 }}
                label={{ value: "Pressure (m)", angle: -90, position: "insideLeft", style: { fontSize: 10 } }}
              />
              <YAxis
                yAxisId="right"
                orientation="right"
                tick={{ fontSize: 10 }}
                label={{ value: "Flow (l/s)", angle: 90, position: "insideRight", style: { fontSize: 10 } }}
              />
              <Tooltip contentStyle={{ fontSize: 11 }} />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Line
                yAxisId="left"
                type="monotone"
                dataKey="pressure"
                stroke="#2563EB"
                strokeWidth={2}
                dot={false}
                name="Pressure"
              />
              <Line
                yAxisId="right"
                type="monotone"
                dataKey="flow"
                stroke="#16A34A"
                strokeWidth={2}
                dot={false}
                name="Flow"
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <EmptyState title="No telemetry" message="No telemetry data available for this sensor." />
      )}

      <div>
        <p className="text-xs font-medium text-gray-500 mb-1">RAG Timeline</p>
        <TimelineStrip history={ragHistory} />
      </div>

      {playbookSteps.length > 0 && (
        <div>
          <p className="text-xs font-medium text-gray-500 mb-2">Response Playbook</p>
          <div className="space-y-2">
            {playbookSteps.map((step: any, i: number) => {
              const actionId = step.action_id || step.step_id || `step-${i}`;
              const current = decisions[actionId];
              return (
                <div key={actionId} className="bg-gray-50 rounded-lg p-3 text-sm">
                  <div className="flex justify-between items-start mb-2">
                    <span className="font-medium">
                      {step.step_order != null ? `${step.step_order}. ` : ""}
                      {step.action || step.description}
                    </span>
                  </div>
                  <div className="flex gap-1.5">
                    {["Accept", "Defer", "Not Applicable"].map((opt) => (
                      <button
                        key={opt}
                        onClick={() => handleDecision(actionId, opt)}
                        className={`px-2.5 py-1 rounded text-xs font-medium transition-colors ${
                          current === opt
                            ? opt === "Accept"
                              ? "bg-green-600 text-white"
                              : opt === "Defer"
                              ? "bg-amber-500 text-white"
                              : "bg-gray-500 text-white"
                            : "bg-white border border-gray-200 text-gray-600 hover:bg-gray-50"
                        }`}
                      >
                        {opt}
                      </button>
                    ))}
                  </div>
                </div>
              );
            })}
            <button
              onClick={handleSavePlaybook}
              disabled={Object.keys(decisions).length === 0 || saveMutation.isPending}
              className="btn-primary w-full mt-2"
            >
              {saveMutation.isPending
                ? "Saving..."
                : saveMutation.isSuccess
                ? "Saved"
                : "Save Decisions"}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
