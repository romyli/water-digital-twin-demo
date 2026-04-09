export default function LoadingSpinner({ message = "Loading..." }: { message?: string }) {
  return (
    <div className="flex flex-col items-center justify-center py-12 text-gray-400">
      <div className="w-8 h-8 border-2 border-gray-200 border-t-water-600 rounded-full animate-spin mb-3" />
      <span className="text-sm">{message}</span>
    </div>
  );
}
