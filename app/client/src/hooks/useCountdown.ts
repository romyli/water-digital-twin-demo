import { useState, useEffect } from "react";
import { differenceInSeconds } from "date-fns";

export function useCountdown(startTime?: string, timeOffset = 0) {
  const [elapsed, setElapsed] = useState("00:00:00");
  const [seconds, setSeconds] = useState(0);

  useEffect(() => {
    if (!startTime) return;
    const start = new Date(startTime);
    const tick = () => {
      // Use demo-world "now" when timeOffset is provided
      const now = timeOffset ? new Date(Date.now() - timeOffset) : new Date();
      const s = differenceInSeconds(now, start);
      setSeconds(s);
      const h = Math.floor(s / 3600);
      const m = Math.floor((s % 3600) / 60);
      const sec = s % 60;
      setElapsed(
        `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(sec).padStart(2, "0")}`
      );
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [startTime, timeOffset]);

  return { elapsed, seconds };
}
