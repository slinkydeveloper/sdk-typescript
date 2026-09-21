/*
 * Copyright (c) 2023-2026 - Restate Software, Inc., Restate GmbH
 *
 * This file is part of the Restate SDK for Node.js/TypeScript,
 * which is released under the MIT license.
 *
 * You can find a copy of the license in file LICENSE in the root
 * directory of this repository or package, or at
 * https://github.com/restatedev/sdk-typescript/blob/main/LICENSE
 */
/* eslint-disable @typescript-eslint/no-unused-vars, @typescript-eslint/require-await */

// -----------------------------------------------------------------------------
// TYPE-LEVEL DEMO for the `restate.state(...)` iface-style state DSL.
//
// This file is not executed; it is type-checked. To verify:
//   cd packages/libs/restate-sdk && npx tsc --noEmit --project tsconfig.demo.json
// Every `const x: T = ...` and `// @ts-expect-error` line is an assertion about
// the types inferred by `implement()`.
// -----------------------------------------------------------------------------

import * as restate from "../src/index.js";
import type { StandardSchemaV1 } from "@restatedev/restate-sdk-core";

// A stand-in for a real Standard Schema (zod/valibot/arktype/...).
type Profile = { name: string; tier: "free" | "pro" };
const ProfileSchema = {} as StandardSchemaV1<Profile>;
type Session = { token: string };
const sessionSerde = restate.serde.json as restate.Serde<Session>;

// =============================================================================
// 1. Declare the state as a reusable descriptor — mirrors `iface.object`.
// =============================================================================
const counterState = restate.state({
  // types only, JSON serde, WITH a default -> get is non-nullable
  count: restate.state.value<number>({ default: 0 }),
  // types only, no default -> get is nullable
  lastActor: restate.state.value<string>(),
  // explicit serde, no default
  session: restate.state.serde<Session>(sessionSerde),
  // Standard Schema: value type inferred from the schema + validated
  profile: restate.state.schema(ProfileSchema),
});

// =============================================================================
// 2. Declare the contract; the state lives ON the iface (like description).
// =============================================================================
const counter = restate.iface.object(
  "counter",
  {
    add: restate.iface.json<number, number>(),
    current: restate.iface.shared.json<void, number>(),
  },
  { state: counterState }
);

// =============================================================================
// 3. Implement it — NO extra `states` field: the ctx type flows from the
//    contract, exactly like handler input/output types do.
// =============================================================================
export const counterImpl = restate.implement(counter, {
  handlers: {
    add: async (ctx, delta) => {
      // default -> non-nullable. This assignment is the assertion.
      const c: number = await ctx.get("count");

      // no default -> nullable. Must NOT be assignable to `number`.
      // @ts-expect-error get("lastActor") is `string | null`, not `string`
      const who: string = await ctx.get("lastActor");
      const whoOk: string | null = await ctx.get("lastActor");

      // schema-inferred value type
      const p: Profile | null = await ctx.get("profile");

      // set is typed against the declared shape
      ctx.set("count", c + delta);
      // @ts-expect-error count is a number, not a string
      ctx.set("count", "nope");

      // @ts-expect-error "unknown" is not a declared state key
      await ctx.get("unknown");

      return c + delta;
    },
    // shared handler gets the (read-only) shared context, still typed
    current: async (ctx) => {
      const c: number = await ctx.get("count");
      // @ts-expect-error set() does not exist on the shared context
      ctx.set("count", 1);
      return c;
    },
  },
});

// =============================================================================
// 4. Backwards compatibility: an iface WITHOUT declared state stays untyped —
//    string keys, explicit value type param, nullable result. (current behavior)
// =============================================================================
const legacy = restate.iface.object("legacy", {
  poke: restate.iface.json<void, void>(),
});

export const legacyImpl = restate.implement(legacy, {
  handlers: {
    poke: async (ctx) => {
      const anyKey: string | null = await ctx.get<string>("whatever");
      ctx.set<number>("n", 1);
    },
  },
});

// =============================================================================
// 5. Workflow: run() gets the writable ctx, other handlers the shared one.
// =============================================================================
const wfState = restate.state({
  status: restate.state.value<"open" | "closed">({ default: "open" }),
});

const paymentWf = restate.iface.workflow(
  "payment",
  {
    run: restate.iface.json<void, string>(),
    peek: restate.iface.shared.json<void, string>(),
  },
  { state: wfState }
);

export const paymentImpl = restate.implement(paymentWf, {
  handlers: {
    run: async (ctx) => {
      const s: "open" | "closed" = await ctx.get("status"); // default -> non-null
      ctx.set("status", "closed");
      return s;
    },
    peek: async (ctx) => {
      const s: "open" | "closed" = await ctx.get("status");
      return s;
    },
  },
});
