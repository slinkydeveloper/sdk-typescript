/*
 * Copyright (c) 2023 - Restate Software, Inc., Restate GmbH
 *
 * This file is part of the Restate SDK for Node.js/TypeScript,
 * which is released under the MIT license.
 *
 * You can find a copy of the license in file LICENSE in the root
 * directory of this repository or package, or at
 * https://github.com/restatedev/sdk-typescript/blob/main/LICENSE
 */

import { describe, expect } from "@jest/globals";
import * as restate from "../src/public_api";
import { TestDriver } from "./testdriver";
import {
  completionMessage,
  END_MESSAGE,
  getStateKeysMessage,
  greetRequest,
  greetResponse,
  inputMessage,
  keyVal,
  outputMessage,
  startMessage,
  suspensionMessage,
} from "./protoutils";
import { TestGreeter, TestResponse } from "../src/generated/proto/test";
import { GetStateKeysEntryMessage_StateKeys } from "../src/generated/proto/protocol";

const INPUT_MESSAGE = inputMessage(greetRequest(""));

function stateKeys(...keys: Array<string>): GetStateKeysEntryMessage_StateKeys {
  return {
    keys: keys.map((b) => Buffer.from(b)),
  };
}

class ListKeys implements TestGreeter {
  async greet(): Promise<TestResponse> {
    const ctx = restate.useKeyedContext(this);

    return {
      greeting: (await ctx.stateKeys()).join(","),
    };
  }
}

describe("ListKeys", () => {
  it("with partial state suspends", async () => {
    const result = await new TestDriver(new ListKeys(), [
      startMessage(1, true, [keyVal("A", "1")]),
      INPUT_MESSAGE,
    ]).run();

    expect(result).toStrictEqual([
      getStateKeysMessage(),
      suspensionMessage([1]),
    ]);
  });

  it("with partial state", async () => {
    const result = await new TestDriver(new ListKeys(), [
      startMessage(1, true, [keyVal("A", "1")]),
      INPUT_MESSAGE,
      completionMessage(
        1,
        GetStateKeysEntryMessage_StateKeys.encode(stateKeys("B", "C")).finish()
      ),
    ]).run();

    expect(result).toStrictEqual([
      getStateKeysMessage(),
      outputMessage(greetResponse("B,C")),
      END_MESSAGE,
    ]);
  });

  it("with complete state", async () => {
    const result = await new TestDriver(new ListKeys(), [
      startMessage(1, false, [keyVal("A", "1")]),
      INPUT_MESSAGE,
    ]).run();

    expect(result).toStrictEqual([
      getStateKeysMessage(["A"]),
      outputMessage(greetResponse("A")),
      END_MESSAGE,
    ]);
  });

  it("replay", async () => {
    const result = await new TestDriver(new ListKeys(), [
      startMessage(1, true, [keyVal("A", "1")]),
      INPUT_MESSAGE,
      getStateKeysMessage(["A", "B", "C"]),
    ]).run();

    expect(result).toStrictEqual([
      outputMessage(greetResponse("A,B,C")),
      END_MESSAGE,
    ]);
  });
});
