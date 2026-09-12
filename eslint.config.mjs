import js from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["dist/**", "node_modules/**", "Trash/**"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    languageOptions: {
      globals: Object.fromEntries(
        [
          "require",
          "module",
          "exports",
          "__dirname",
          "__filename",
          "process",
          "Buffer",
          "console",
          "setTimeout",
          "clearTimeout",
          "setInterval",
          "clearInterval",
          "setImmediate",
          "clearImmediate",
          "URL",
          "URLSearchParams",
          "AbortController",
          "AbortSignal",
          "TextEncoder",
          "TextDecoder",
          "fetch",
          "global",
          "performance",
        ].map((name) => [name, "readonly"]),
      ),
    },
    rules: {
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/no-unused-vars": "off",
      "@typescript-eslint/no-require-imports": "off",
      "no-unused-vars": "off",
      "no-empty": ["error", { allowEmptyCatch: true }],
      "no-redeclare": ["error", { builtinGlobals: false }],
    },
  },
  { files: ["**/*.js"], languageOptions: { sourceType: "commonjs" } },
  // These validators deliberately recognize forbidden control characters.
  {
    files: ["src/server.ts", "src/validation.ts"],
    rules: { "no-control-regex": "off" },
  },
  // Negative contract fixtures intentionally contain malformed arrays.
  { files: ["test/**/*.js"], rules: { "no-sparse-arrays": "off" } },
);
