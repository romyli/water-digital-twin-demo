import { GenieChat } from "@databricks/appkit-ui/react";

export default function GenieNativePage() {
  return (
    <div className="flex flex-col h-full min-h-0">
      {/* Header */}
      <div className="shrink-0 border-b border-gray-200 bg-white px-6 py-3">
        <div className="max-w-4xl mx-auto flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-water-700 text-white flex items-center justify-center shrink-0">
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.5}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904 9 18.75l-.813-2.846a4.5 4.5 0 0 0-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 0 0 3.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 0 0 3.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 0 0-3.09 3.09ZM18.259 8.715 18 9.75l-.259-1.035a3.375 3.375 0 0 0-2.455-2.456L14.25 6l1.036-.259a3.375 3.375 0 0 0 2.455-2.456L18 2.25l.259 1.035a3.375 3.375 0 0 0 2.455 2.456L21.75 6l-1.036.259a3.375 3.375 0 0 0-2.455 2.456Z" />
            </svg>
          </div>
          <div>
            <h1 className="text-sm font-semibold text-gray-900">Network Operations Assistant</h1>
            <p className="text-[11px] text-gray-500">Powered by Databricks AI/BI Genie</p>
          </div>
        </div>
      </div>

      {/* GenieChat fills remaining space */}
      <div className="flex-1 min-h-0 overflow-hidden bg-white">
        <GenieChat
          alias="default"
          className="h-full"
          placeholder="e.g. Which DMAs are red? Show pressure trends for DEMO_DMA_01..."
        />
      </div>
    </div>
  );
}
