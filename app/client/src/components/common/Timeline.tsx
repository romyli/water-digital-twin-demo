import { format } from "date-fns";

export type TimelineNode = {
  label: string;
  description?: string;
  timestamp?: string;
  status: "done" | "outstanding" | "in-progress";
};

const NODE_COLORS: Record<string, string> = {
  done: "bg-green-500",
  outstanding: "bg-amber-500",
  "in-progress": "bg-blue-500",
};

const BORDER_COLORS: Record<string, string> = {
  done: "border-green-500",
  outstanding: "border-amber-500",
  "in-progress": "border-blue-500",
};

export default function Timeline({ nodes }: { nodes: TimelineNode[] }) {
  if (nodes.length === 0) return null;
  return (
    <div className="relative pl-6">
      {/* Continuous left border */}
      <div className="absolute left-[9px] top-2 bottom-2 w-0.5 bg-gray-200" />
      <div className="space-y-4">
        {nodes.map((node, i) => (
          <div key={i} className="relative flex items-start gap-3">
            {/* Node circle */}
            <div
              className={`absolute -left-6 mt-0.5 w-[18px] h-[18px] rounded-full border-2 border-white ${NODE_COLORS[node.status]} ring-2 ${BORDER_COLORS[node.status]} flex-shrink-0`}
            />
            <div className="min-w-0">
              <p className="text-sm font-medium text-gray-900">{node.label}</p>
              {node.description && (
                <p className="text-xs text-gray-500 mt-0.5">{node.description}</p>
              )}
              {node.timestamp && (
                <p className="text-xs text-gray-400 mt-0.5">
                  {format(new Date(node.timestamp), "HH:mm")}
                </p>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
