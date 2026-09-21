/*
 * Copyright (c) 2023-2024 - Restate Software, Inc., Restate GmbH
 *
 * This file is part of the Restate SDK for Node.js/TypeScript,
 * which is released under the MIT license.
 *
 * You can find a copy of the license in file LICENSE in the root
 * directory of this repository or package, or at
 * https://github.com/restatedev/sdk-typescript/blob/main/LICENSE
 */

import {
  createObjectHandler,
  createObjectSharedHandler,
  iface,
  implement,
  object,
  state,
  type ObjectContext,
  type ObjectSharedContext,
  serde,
  serve,
} from "@restatedev/restate-sdk";

export const counter = object({
  name: "counter",
  handlers: {
    /**
     * Add amount to the currently stored count
     */
    add: async (ctx: ObjectContext, amount: number) => {
      const current = await ctx.get<number>("count");
      const updated = (current ?? 0) + amount;
      ctx.set("count", updated);
      return updated;
    },

    /**
     * Get the current amount.
     *
     * Notice that VirtualObjects can have "shared" handlers.
     * These handlers can be executed concurrently to the exclusive handlers (i.e. add)
     * But they can not modify the state (set is missing from the ctx).
     */
    current: createObjectSharedHandler(
      async (ctx: ObjectSharedContext): Promise<number> => {
        return (await ctx.get("count")) ?? 0;
      }
    ),

    /**
     * Handlers (shared or exclusive) can be configured to bypass JSON serialization,
     * by specifying the input (accept) and output (contentType) content types.
     *
     * to call that handler with binary data, you can use the following curl command:
     * curl -X POST -H "Content-Type: application/octet-stream" --data-binary 'hello' ${RESTATE_INGRESS_URL}/counter/mykey/binary
     */
    binary: createObjectHandler(
      {
        input: serde.binary,
        output: serde.binary,
      },
      async (ctx: ObjectContext, data: Uint8Array) => {
        // console.log("Received binary data", data);
        return data;
      }
    ),
  },
});

export type Counter = typeof counter;

// ---------------------------------------------------------------------------
// The same counter, but with TYPED STATE (prototype).
//
// Instead of typing every handler's `ctx` by hand and remembering which state
// keys exist, we declare the object's state once with `restate.state(...)` and
// hang it on the contract (just like `iface` declares the handlers). The
// handler `ctx` is then inferred from the contract:
//   - `ctx.get("count")` is `number` (not `number | null`) because `count`
//     declares a default, so no more `?? 0`;
//   - `ctx.set("count", ...)` is checked against the declared type;
//   - `ctx.get("typo")` doesn't compile.
// ---------------------------------------------------------------------------

const counterState = state({
  // Declaring a default makes `ctx.get("count")` non-nullable (typed `number`).
  //
  // ⚠️ Prototype: the default is currently enforced at the TYPE level only.
  // Runtime resolution is still a TODO, so an unset key still reads back as null
  // at runtime. This demo stays correct because the default is 0 and, in JS,
  // `null + n === n`.
  count: state.value<number>({ default: 0 }),
});

const typedCounterContract = iface.object(
  "typedCounter",
  {
    add: iface.json<number, number>(),
    current: iface.shared.json<void, number>(),
  },
  { state: counterState }
);

export const typedCounter = implement(typedCounterContract, {
  handlers: {
    // No explicit `ctx` type: it is inferred from the contract as
    // ObjectContext<{ count: number }, "count">.
    add: async (ctx, amount) => {
      const current = await ctx.get("count"); // typed `number`
      const updated = current + amount;
      ctx.set("count", updated);
      return updated;
    },

    // A shared handler gets the read-only context (no `set`), still typed.
    // The return type `number` compiles precisely because `count` has a default;
    // without one, `ctx.get("count")` would be `number | null` and mismatch.
    current: async (ctx) => ctx.get("count"),
  },
});

export type TypedCounter = typeof typedCounter;

serve({ services: [counter, typedCounter] });
