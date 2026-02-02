import { useEffect, useRef } from "react";

/**
 * A custom hook that sets up an interval and clears it when the component unmounts.
 * It uses a ref to store the latest callback to avoid stale closures.
 *
 * @param callback The function to be called every interval.
 * @param delay The delay in milliseconds. If null, the interval is paused.
 */
export function useInterval(callback: () => void, delay: number | null) {
  const savedCallback = useRef<() => void>(undefined);

  // Remember the latest callback.
  useEffect(() => {
    savedCallback.current = callback;
  }, [callback]);

  // Set up the interval.
  useEffect(() => {
    function tick() {
      if (savedCallback.current) {
        savedCallback.current();
      }
    }
    if (delay !== null) {
      const id = setInterval(tick, delay);
      return () => clearInterval(id);
    }
  }, [delay]);
}
