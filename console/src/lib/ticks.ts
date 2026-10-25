/**
 * Axis ticks for the console's charts: round values on the value axis, round instants on the time
 * axis. Pure and import-free, so `node --test` loads it without the bundler.
 */

/** A round step near `span / count`: 1, 2, 2.5 or 5 times a power of ten. */
export function niceStep(span: number, count: number): number {
  if (!(span > 0) || !(count > 0)) return 1;
  const raw = span / count;
  const magnitude = 10 ** Math.floor(Math.log10(raw));
  const fraction = raw / magnitude;
  const nice = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 2.5 ? 2.5 : fraction <= 5 ? 5 : 10;
  return nice * magnitude;
}

export interface ValueAxis {
  min: number;
  max: number;
  ticks: number[];
}

/**
 * A value axis covering `values`, with about `count` round ticks.
 *
 * `zero` keeps zero on the axis, which is right for a rate — half the height should mean half the
 * rate. `auto` keeps zero only when the values come near it, and otherwise fits the axis to them,
 * which is what makes a slope visible in a large number that changes slowly: a lag falling from
 * 108M to 104M is a line across the chart instead of a flat line at its top.
 */
export function valueAxis(values: number[], options: { baseline: 'zero' | 'auto'; count?: number }): ValueAxis {
  const count = options.count ?? 4;
  const finite = values.filter((value) => Number.isFinite(value));
  let low = finite.length > 0 ? Math.min(...finite) : 0;
  let high = finite.length > 0 ? Math.max(...finite) : 1;

  if (options.baseline === 'zero' || (low >= 0 && low <= high * 0.2)) low = Math.min(low, 0);
  if (high === low) {
    // A flat series: give it room above, and below unless it sits on zero.
    const pad = Math.abs(high) * 0.05 || 1;
    high += pad;
    if (low !== 0) low -= pad;
  } else if (low !== 0) {
    const pad = (high - low) * 0.08;
    low -= pad;
    high += pad;
  }

  const step = niceStep(high - low, count);
  const min = low === 0 ? 0 : Math.floor(low / step) * step;
  const max = Math.ceil(high / step) * step;
  const ticks: number[] = [];
  for (let tick = min; tick <= max + step / 1e6; tick += step) ticks.push(Number(tick.toPrecision(12)));
  return { min, max, ticks };
}

const TIME_STEPS = [
  5_000, 10_000, 15_000, 30_000, 60_000, 120_000, 300_000, 600_000, 900_000, 1_800_000, 3_600_000, 7_200_000,
  10_800_000, 21_600_000, 43_200_000, 86_400_000,
];

/**
 * Round instants between `from` and `to`, at most `maxTicks` of them.
 *
 * Steps are whole seconds, minutes and hours, aligned in local time, so labels read 14:30, 14:35,
 * 14:40 rather than 14:31:17. `step` is returned too, so labels can show seconds only when the
 * step is shorter than a minute.
 */
export function timeTicks(from: number, to: number, maxTicks: number): { ticks: number[]; step: number } {
  const span = Math.max(1, to - from);
  const step = TIME_STEPS.find((candidate) => span / candidate <= maxTicks) ?? (TIME_STEPS.at(-1) as number);
  // Align to the step in local time: a local midnight is a multiple of every step up to a day.
  const offset = new Date(from).getTimezoneOffset() * 60_000;
  const first = Math.ceil((from - offset) / step) * step + offset;
  const ticks: number[] = [];
  for (let tick = first; tick <= to; tick += step) ticks.push(tick);
  return { ticks, step };
}
