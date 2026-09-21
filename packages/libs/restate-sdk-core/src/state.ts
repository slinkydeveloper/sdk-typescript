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
/* eslint-disable @typescript-eslint/no-explicit-any */

import type { Serde } from "./serde_api.js";
import { serde } from "./serde_api.js";
import type { StandardSchemaV1 } from "./standard_schema.js";

// =============================================================================
// state — declare the key-value state of a Virtual Object / Workflow as a
// reusable, typed descriptor, mirroring how `iface` declares handlers.
//
//   iface (per handler)          state (per key)
//   ------------------------     ----------------------------------
//   iface.json<I, O>()           state.value<T>()          // + { default }
//   iface.serdes({ ... })        state.serde<T>(serde)     // + { default }
//   iface.schemas({ ... })       state.schema(zodSchema)   // + { default }
//
// A `StateDescriptor` is the per-key analogue of `Descriptor._handlers`: it
// carries, for each key, the value type, an optional serde/schema and whether a
// default is declared. `implement()` (and, in future, `object()`/`workflow()`)
// reads it to type the handler `ctx` — including dropping `| null` from
// `ctx.get(key)` for keys that declare a default (see issue #566).
// =============================================================================

/**
 * Minimal descriptor stored per state key inside a {@link StateDescriptor}.
 *
 * `HasDefault` is a phantom flag recovered by the SDK to decide whether
 * `ctx.get(key)` can drop `| null`.
 */
export type StateKeyDescriptor<
  T = any,
  HasDefault extends boolean = boolean,
> = {
  /** @internal serde used to (de)serialize this key; JSON when absent */
  readonly _serde?: Serde<T>;
  /** @internal default value returned by `ctx.get` when the key is unset */
  readonly _default?: T;
  /** @internal phantom: `true` when a default was declared */
  readonly _hasDefault?: HasDefault;
};

/** A map of state key name -> {@link StateKeyDescriptor}. */
export type StateKeys = Record<string, StateKeyDescriptor>;

/**
 * The typed state contract of a Virtual Object / Workflow: one
 * {@link StateKeyDescriptor} per key. Build it with {@link state}.
 */
export type StateDescriptor<K extends StateKeys = StateKeys> = {
  readonly _keys: K;
};

/**
 * The "no typed state declared" sentinel. Its key map is the empty object type,
 * so `keyof` is `never`, which the SDK maps back to the untyped context.
 */
// eslint-disable-next-line @typescript-eslint/ban-types
export type EmptyState = StateDescriptor<{}>;

/** Recover the value type declared for a single key. */
export type InferStateValue<D> =
  D extends StateKeyDescriptor<infer T, any> ? T : never;

/** Recover whether a single key declares a default. */
export type StateKeyHasDefault<D> =
  D extends StateKeyDescriptor<any, infer HD> ? HD : false;

function makeStateKeyDescriptor<T, HasDefault extends boolean>(
  serdeImpl: Serde<T> | undefined,
  def: T | undefined,
  hasDefault: HasDefault
): StateKeyDescriptor<T, HasDefault> {
  return { _serde: serdeImpl, _default: def, _hasDefault: hasDefault };
}

/** The shape of the {@link state} declaration API. */
export interface StateFactory {
  /** Group per-key declarations into a {@link StateDescriptor}. */
  <K extends StateKeys>(keys: K): StateDescriptor<K>;

  /** `value<T>()` — type only, default JSON serde. Optionally a `default`. */
  value<T>(): StateKeyDescriptor<T, false>;
  value<T>(opts: { default: T }): StateKeyDescriptor<T, true>;

  /** `serde<T>(serde)` — explicit {@link Serde}. Optionally a `default`. */
  serde<T>(serdeImpl: Serde<T>): StateKeyDescriptor<T, false>;
  serde<T>(
    serdeImpl: Serde<T>,
    opts: { default: T }
  ): StateKeyDescriptor<T, true>;

  /**
   * `schema(zodSchema)` — Standard Schema. The value type is *inferred* from
   * the schema and the value is validated at (de)serialization time.
   */
  schema<S extends StandardSchemaV1<any>>(
    schema: S
  ): StateKeyDescriptor<StandardSchemaV1.InferOutput<S>, false>;
  schema<S extends StandardSchemaV1<any>>(
    schema: S,
    opts: { default: StandardSchemaV1.InferOutput<S> }
  ): StateKeyDescriptor<StandardSchemaV1.InferOutput<S>, true>;
}

/**
 * Declare the typed key-value state of a Virtual Object / Workflow.
 *
 * @example
 * ```ts
 * const counterState = state({
 *   count:   state.value<number>({ default: 0 }),   // ctx.get("count") -> number
 *   session: state.serde<Session>(sessionSerde),    // ctx.get("session") -> Session | null
 *   profile: state.schema(ProfileSchema),           // type inferred + validated
 * });
 *
 * const counter = iface.object("counter", { add: iface.json<number, number>() }, {
 *   state: counterState,
 * });
 * ```
 */
export const state: StateFactory = Object.assign(
  function group<K extends StateKeys>(keys: K): StateDescriptor<K> {
    return { _keys: keys };
  },
  {
    value: function <T>(opts?: { default: T }): StateKeyDescriptor<T, any> {
      return makeStateKeyDescriptor<T, any>(
        undefined,
        opts?.default,
        opts !== undefined && "default" in opts
      );
    },
    serde: function <T>(
      serdeImpl: Serde<T>,
      opts?: { default: T }
    ): StateKeyDescriptor<T, any> {
      return makeStateKeyDescriptor<T, any>(
        serdeImpl,
        opts?.default,
        opts !== undefined && "default" in opts
      );
    },
    schema: function <S extends StandardSchemaV1<any>>(
      schema: S,
      opts?: { default: StandardSchemaV1.InferOutput<S> }
    ): StateKeyDescriptor<StandardSchemaV1.InferOutput<S>, any> {
      return makeStateKeyDescriptor<StandardSchemaV1.InferOutput<S>, any>(
        serde.schema<StandardSchemaV1.InferOutput<S>>(schema),
        opts?.default,
        opts !== undefined && "default" in opts
      );
    },
  }
);
