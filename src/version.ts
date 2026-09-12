const packageMetadata = require("../package.json") as { version?: unknown };

if (
  typeof packageMetadata.version !== "string" ||
  packageMetadata.version.length === 0
) {
  throw new Error("package.json must define a non-empty version");
}

/** Version reported by the server and kept in sync with package.json. */
export const YASD_VERSION = packageMetadata.version;
