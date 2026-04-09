import type { ReactNode } from "react";

export default function EmptyState({
  title = "No data",
  message = "Nothing to display right now.",
  icon,
}: {
  title?: string;
  message?: string;
  icon?: ReactNode;
}) {
  return (
    <div className="flex flex-col items-center justify-center py-10 text-center">
      {icon && <div className="text-4xl mb-3 text-gray-300">{icon}</div>}
      <h3 className="text-sm font-semibold text-gray-500 mb-1">{title}</h3>
      <p className="text-xs text-gray-400 max-w-xs">{message}</p>
    </div>
  );
}
