/**
 * Converts snake_case or UPPER_SNAKE to Title Case.
 * e.g. "dialysis_home" → "Dialysis Home", "low_pressure" → "Low Pressure"
 */
export function humanize(str?: string): string {
  if (!str) return "";
  return str
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

/**
 * Relative time that stays granular beyond 24h.
 * Returns e.g. "5m ago", "3h ago", "48h ago" instead of "2 days ago".
 */
export function relativeTimeShort(date: Date): string {
  const diffMs = Date.now() - date.getTime();
  const diffSec = Math.floor(diffMs / 1000);
  if (diffSec < 60) return `${diffSec}s ago`;
  const diffMin = Math.floor(diffSec / 60);
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffHr = Math.floor(diffMin / 60);
  return `${diffHr}h ago`;
}
