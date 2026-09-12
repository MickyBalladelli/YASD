/**
 * Compare JSON-shaped values by structure. Object key insertion order does
 * not affect equality; array order still does. The iterative walk avoids
 * JSON.stringify's ordering and recursion hazards.
 */
export function structuralEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;

  const pending: Array<[unknown, unknown]> = [[a, b]];
  const compared = new WeakMap<object, WeakSet<object>>();

  try {
    while (pending.length > 0) {
      const pair = pending.pop() as [unknown, unknown];
      const [left, right] = pair;
      if (left === right) continue;

      if (
        left === null ||
        right === null ||
        typeof left !== "object" ||
        typeof right !== "object"
      ) {
        return false;
      }

      let rightValues = compared.get(left);
      if (!rightValues) {
        rightValues = new WeakSet<object>();
        compared.set(left, rightValues);
      }
      if (rightValues.has(right)) continue;
      rightValues.add(right);

      const leftArray = Array.isArray(left);
      if (leftArray !== Array.isArray(right)) return false;

      if (leftArray) {
        const leftItems = left as unknown[];
        const rightItems = right as unknown[];
        if (leftItems.length !== rightItems.length) return false;
        for (let i = 0; i < leftItems.length; i++) {
          const leftHas = Object.prototype.hasOwnProperty.call(leftItems, i);
          const rightHas = Object.prototype.hasOwnProperty.call(rightItems, i);
          if (leftHas !== rightHas) return false;
          if (leftHas) pending.push([leftItems[i], rightItems[i]]);
        }
        continue;
      }

      const leftObject = left as Record<string, unknown>;
      const rightObject = right as Record<string, unknown>;
      const leftKeys = Object.keys(leftObject);
      const rightKeys = Object.keys(rightObject);
      if (leftKeys.length !== rightKeys.length) return false;

      for (const key of leftKeys) {
        if (!Object.prototype.hasOwnProperty.call(rightObject, key))
          return false;
        pending.push([leftObject[key], rightObject[key]]);
      }
    }
    return true;
  } catch {
    // A caller-supplied object may expose throwing getters or other unusual
    // behavior. Treat it as unequal instead of allowing comparison to throw.
    return false;
  }
}
