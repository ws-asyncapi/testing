import { describe, expect, it } from "bun:test";
import { harnessDriver, runConformance } from "../src/index.ts";

// Run the full protocol conformance contract over the in-memory harness.
// (Adapter repos run the same suite over real sockets; backplane-redis adds the
// Redis backplane variant for cross-node + recovery.)
runConformance(harnessDriver, { describe, it, expect });
