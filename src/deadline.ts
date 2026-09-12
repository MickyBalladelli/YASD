import { DatabaseError } from "./errors";
/** Deadline racing never retries work. onTimeout must prevent any future dispatch. */
export async function deadline<T>(
  work: Promise<T>,
  ms: number,
  onTimeout: () => void = () => undefined,
): Promise<T> {
  if (ms <= 0) return work;
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      work,
      new Promise<T>((_, reject) => {
        timer = setTimeout(() => {
          onTimeout();
          reject(
            new DatabaseError(
              "operation deadline exceeded; outcome may be unknown",
              "TIMEOUT",
            ),
          );
        }, ms);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}
