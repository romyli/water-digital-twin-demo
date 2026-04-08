import React, { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { fetchComms, addComms } from "../api";
import { format } from "date-fns";
import LoadingSpinner from "./common/LoadingSpinner";
import EmptyState from "./common/EmptyState";

export default function CommunicationsLog({ incidentId, initialComms }) {
  const queryClient = useQueryClient();
  const [showForm, setShowForm] = useState(false);
  const [formData, setFormData] = useState({
    channel: "email",
    recipient: "",
    message: "",
  });

  const { data, isLoading } = useQuery({
    queryKey: ["comms", incidentId],
    queryFn: () => fetchComms(incidentId),
    enabled: !!incidentId && !initialComms,
    initialData: initialComms ? { communications: initialComms } : undefined,
  });

  const comms = data?.communications || initialComms || [];

  const mutation = useMutation({
    mutationFn: (entry) => addComms(incidentId, entry),
    onSuccess: () => {
      queryClient.invalidateQueries(["comms", incidentId]);
      queryClient.invalidateQueries(["handover", incidentId]);
      setFormData({ channel: "email", recipient: "", message: "" });
      setShowForm(false);
    },
  });

  const handleSubmit = (e) => {
    e.preventDefault();
    if (!formData.message.trim()) return;
    mutation.mutate(formData);
  };

  const channelLabel = (ch) => {
    const labels = {
      email: "Email",
      phone: "Phone",
      sms: "SMS",
      system: "System",
      radio: "Radio",
      teams: "Teams",
    };
    return labels[ch] || ch;
  };

  return (
    <div className="card">
      <div className="flex items-center justify-between mb-3">
        <h2 className="panel-title mb-0">Communications Log</h2>
        <button
          onClick={() => setShowForm(!showForm)}
          className="btn-secondary text-xs"
        >
          {showForm ? "Cancel" : "+ Add Entry"}
        </button>
      </div>

      {/* Add form */}
      {showForm && (
        <form onSubmit={handleSubmit} className="bg-gray-50 rounded-lg p-3 mb-4 space-y-2">
          <div className="flex gap-2">
            <select
              value={formData.channel}
              onChange={(e) => setFormData({ ...formData, channel: e.target.value })}
              className="text-sm border border-gray-200 rounded-lg px-2 py-1.5 bg-white"
            >
              <option value="email">Email</option>
              <option value="phone">Phone</option>
              <option value="sms">SMS</option>
              <option value="radio">Radio</option>
              <option value="teams">Teams</option>
              <option value="system">System</option>
            </select>
            <input
              type="text"
              placeholder="Recipient (optional)"
              value={formData.recipient}
              onChange={(e) => setFormData({ ...formData, recipient: e.target.value })}
              className="flex-1 text-sm border border-gray-200 rounded-lg px-3 py-1.5"
            />
          </div>
          <textarea
            placeholder="Message content..."
            value={formData.message}
            onChange={(e) => setFormData({ ...formData, message: e.target.value })}
            rows={2}
            className="w-full text-sm border border-gray-200 rounded-lg px-3 py-2 resize-none"
          />
          <div className="flex justify-end">
            <button
              type="submit"
              disabled={!formData.message.trim() || mutation.isPending}
              className="btn-primary text-xs"
            >
              {mutation.isPending ? "Sending..." : "Add Entry"}
            </button>
          </div>
        </form>
      )}

      {/* Comms list */}
      {isLoading ? (
        <LoadingSpinner message="Loading communications..." />
      ) : comms.length === 0 ? (
        <EmptyState
          title="No communications"
          message="No communication records for this incident."
        />
      ) : (
        <div className="space-y-2 max-h-64 overflow-y-auto">
          {comms.map((c, i) => (
            <div key={i} className="bg-gray-50 rounded-lg px-3 py-2.5 text-sm">
              <div className="flex items-center justify-between mb-1">
                <div className="flex items-center gap-2">
                  <span className="badge bg-blue-100 text-blue-800">
                    {channelLabel(c.channel)}
                  </span>
                  {c.recipient && (
                    <span className="text-gray-500 text-xs">{c.recipient}</span>
                  )}
                </div>
                <span className="text-xs text-gray-400">
                  {c.sent_at
                    ? format(new Date(c.sent_at), "dd MMM HH:mm")
                    : "—"}
                </span>
              </div>
              <p className="text-gray-700 text-xs">{c.message}</p>
              {c.sent_by && (
                <p className="text-gray-400 text-xs mt-0.5">by {c.sent_by}</p>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
