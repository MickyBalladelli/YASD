// Distinguish a truncated JSON prefix from corrupt JSON without depending on V8 messages.
// Used only on a malformed, unterminated final AOF record.
export function isIncompleteJson(text: string): boolean {
  let at = 0;
  const incomplete = Symbol('incomplete');
  const invalid = Symbol('invalid');
  const whitespace = (): void => { while (/\s/.test(text[at] ?? '') && at < text.length) at++; };
  const need = (char: string): void => {
    whitespace();
    if (at === text.length) throw incomplete;
    if (text[at++] !== char) throw invalid;
  };
  const string = (): void => {
    need('"');
    while (at < text.length) {
      const char = text[at++];
      if (char === '"') return;
      if (char.charCodeAt(0) < 32) throw invalid;
      if (char === '\\') {
        if (at === text.length) throw incomplete;
        const escaped = text[at++];
        if (escaped === 'u') {
          for (let i = 0; i < 4; i++) {
            if (at === text.length) throw incomplete;
            if (!/[0-9a-fA-F]/.test(text[at++])) throw invalid;
          }
        } else if (!'"\\/bfnrt'.includes(escaped)) throw invalid;
      }
    }
    throw incomplete;
  };
  const value = (depth: number): void => {
    if (depth > 256) throw invalid;
    whitespace();
    if (at === text.length) throw incomplete;
    const first = text[at];
    if (first === '"') return string();
    if (first === '{' || first === '[') {
      at++;
      const end = first === '{' ? '}' : ']';
      whitespace();
      if (text[at] === end) { at++; return; }
      for (;;) {
        if (first === '{') { string(); need(':'); }
        value(depth + 1);
        whitespace();
        if (at === text.length) throw incomplete;
        if (text[at] === end) { at++; return; }
        need(',');
      }
    }
    const literal = first === 't' ? 'true' : first === 'f' ? 'false' : first === 'n' ? 'null' : undefined;
    if (literal) {
      for (const char of literal) {
        if (at === text.length) throw incomplete;
        if (text[at++] !== char) throw invalid;
      }
      return;
    }
    const match = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/.exec(text.slice(at));
    if (!match) {
      if (text.slice(at) === '-') throw incomplete;
      throw invalid;
    }
    at += match[0].length;
    if (/^[.eE][+-]?\d*$/.test(text.slice(at))) throw incomplete;
  };
  try {
    value(0);
    whitespace();
    return false;
  } catch (error) {
    return error === incomplete;
  }
}
