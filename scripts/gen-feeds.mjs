// Generates src/feeds.ts from feeds.txt
import { readFileSync, writeFileSync } from "node:fs";

const raw = readFileSync("feeds.txt", "utf-8");
const feeds = raw
  .split(/\r?\n/)
  .map((l) => l.trim())
  .filter((l) => l && !l.startsWith("#"));

const out = `// Generated from feeds.txt - do not edit manually.
// Run: node scripts/gen-feeds.mjs to regenerate.
export const FEED_LIST: string[] = ${JSON.stringify(feeds)};
`;

writeFileSync("src/feeds.ts", out);
console.log(`Wrote ${feeds.length} feeds to src/feeds.ts`);
