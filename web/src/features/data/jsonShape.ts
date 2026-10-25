import type { RunExportShape } from '@shared/types';

const SHAPE_KEY = 'console.data.jsonShape';

/** How JSON leaves the Data tab — exports and the row drawer's copy alike — remembered per browser. */
export function readJsonShape(): RunExportShape {
  try {
    return localStorage.getItem(SHAPE_KEY) === 'flat' ? 'flat' : 'nested';
  } catch {
    return 'nested';
  }
}

export function writeJsonShape(shape: RunExportShape): void {
  try {
    localStorage.setItem(SHAPE_KEY, shape);
  } catch {
    /* not remembering a preference is not worth failing over */
  }
}
