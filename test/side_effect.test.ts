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
  inputMessage,
  outputMessage,
  sideEffectMessage,
  startMessage,
  greetRequest,
  greetResponse,
  invokeMessage,
  getAwakeableId,
  backgroundInvokeMessage,
  suspensionMessage,
  checkTerminalError,
  checkJournalMismatchError,
  failureWithTerminal,
  ackMessage,
  setStateMessage,
  Latch,
  END_MESSAGE,
} from "./protoutils";
import {
  TestGreeter,
  TestGreeterClientImpl,
  TestRequest,
  TestResponse,
} from "../src/generated/proto/test";
import { ErrorCodes, TerminalError } from "../src/types/errors";
import { setTimeout } from "timers/promises";
import { FIXED_DELAY } from "../src/utils/public_utils";
import exp = require("node:constants");
import { SLEEP_ENTRY_MESSAGE_TYPE } from "../src/types/protocol";
import { Empty } from "../src/generated/google/protobuf/empty";

class SideEffectGreeter implements TestGreeter {
  constructor(readonly sideEffectOutput: unknown) {}

  async greet(): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    // state
    const response = await ctx.sideEffect(async () => {
      return this.sideEffectOutput;
    });

    return TestResponse.create({ greeting: `Hello ${response}` });
  }
}

describe("SideEffectGreeter", () => {
  it("sends message to runtime", async () => {
    const result = await new TestDriver(new SideEffectGreeter("Francesco"), [
      startMessage(),
      inputMessage(greetRequest("Till")),
    ]).run();

    expect(result).toStrictEqual([
      sideEffectMessage("Francesco"),
      suspensionMessage([1]),
    ]);
  });

  it("sends message to runtime for undefined result", async () => {
    const result = await new TestDriver(new SideEffectGreeter(undefined), [
      startMessage(),
      inputMessage(greetRequest("Till")),
    ]).run();

    expect(result).toStrictEqual([
      sideEffectMessage(undefined),
      suspensionMessage([1]),
    ]);
  });

  it("sends message to runtime for empty object", async () => {
    const result = await new TestDriver(new SideEffectGreeter({}), [
      startMessage(),
      inputMessage(greetRequest("Till")),
    ]).run();

    expect(result).toStrictEqual([
      sideEffectMessage({}),
      suspensionMessage([1]),
    ]);
  });

  it("handles acks", async () => {
    const result = await new TestDriver(new SideEffectGreeter("Francesco"), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      ackMessage(1),
    ]).run();

    expect(result).toStrictEqual([
      sideEffectMessage("Francesco"),
      outputMessage(greetResponse("Hello Francesco")),
      END_MESSAGE,
    ]);
  });

  it("handles replay with undefined value", async () => {
    const result = await new TestDriver(new SideEffectGreeter("Francesco"), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      sideEffectMessage(),
    ]).run();

    expect(result).toStrictEqual([
      outputMessage(greetResponse("Hello undefined")),
      END_MESSAGE,
    ]);
  });

  it("handles replay with empty object value", async () => {
    const result = await new TestDriver(new SideEffectGreeter({}), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      sideEffectMessage(),
    ]).run();

    expect(result).toStrictEqual([
      outputMessage(greetResponse("Hello undefined")),
      END_MESSAGE,
    ]);
  });

  it("handles replay with value", async () => {
    const result = await new TestDriver(new SideEffectGreeter("Francesco"), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      sideEffectMessage("Francesco"),
    ]).run();

    expect(result).toStrictEqual([
      outputMessage(greetResponse("Hello Francesco")),
      END_MESSAGE,
    ]);
  });

  it("handles replay with empty string", async () => {
    const result = await new TestDriver(new SideEffectGreeter("Francesco"), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      sideEffectMessage(""),
    ]).run();

    expect(result).toStrictEqual([
      outputMessage(greetResponse("Hello ")),
      END_MESSAGE,
    ]);
  });

  it("handles replay with failure", async () => {
    const result = await new TestDriver(new SideEffectGreeter("Francesco"), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      sideEffectMessage(
        undefined,
        failureWithTerminal(true, "Something went wrong.")
      ),
    ]).run();

    checkTerminalError(result[0], "Something went wrong.");
  });

  it("fails on journal mismatch. Completed with invoke.", async () => {
    const result = await new TestDriver(new SideEffectGreeter("Francesco"), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      invokeMessage(
        "test.TestGreeter",
        "Greet",
        greetRequest("Francesco"),
        greetResponse("FRANCESCO")
      ), // should have been side effect
    ]).run();

    checkJournalMismatchError(result[0]);
  });
});

class SideEffectAndInvokeGreeter implements TestGreeter {
  async greet(): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    const client = new TestGreeterClientImpl(ctx);

    // state
    const result = await ctx.sideEffect<string>(async () => "abcd");

    const greetingPromise1 = await client.greet(
      TestRequest.create({ name: result })
    );

    return TestResponse.create({
      greeting: `Hello ${greetingPromise1.greeting}`,
    });
  }
}

// Checks if the side effect flag is put back to false when we are in replay and do not execute the side effect
describe("SideEffectAndInvokeGreeter", () => {
  it("handles replay and then invoke.", async () => {
    const result = await new TestDriver(new SideEffectAndInvokeGreeter(), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      sideEffectMessage("abcd"),
      completionMessage(2, greetResponse("FRANCESCO")),
    ]).run();

    expect(result).toStrictEqual([
      invokeMessage("test.TestGreeter", "Greet", greetRequest("abcd")),
      outputMessage(greetResponse("Hello FRANCESCO")),
      END_MESSAGE,
    ]);
  });

  it("handles completion and then invoke", async () => {
    const result = await new TestDriver(new SideEffectAndInvokeGreeter(), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      ackMessage(1),
      completionMessage(2, greetResponse("FRANCESCO")),
    ]).run();

    expect(result).toStrictEqual([
      sideEffectMessage("abcd"),
      invokeMessage("test.TestGreeter", "Greet", greetRequest("abcd")),
      outputMessage(greetResponse("Hello FRANCESCO")),
      END_MESSAGE,
    ]);
  });
});

class SideEffectAndOneWayCallGreeter implements TestGreeter {
  async greet(): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    const client = new TestGreeterClientImpl(ctx);

    // state
    const result = await ctx.sideEffect<string>(async () => "abcd");

    await ctx.oneWayCall(() =>
      client.greet(TestRequest.create({ name: result }))
    );
    const response = await client.greet(TestRequest.create({ name: result }));

    return TestResponse.create({ greeting: `Hello ${response.greeting}` });
  }
}

// Checks if the side effect flag is put back to false when we are in replay and do not execute the side effect
describe("SideEffectAndOneWayCallGreeter", () => {
  it("handles completion and then invoke", async () => {
    const result = await new TestDriver(new SideEffectAndOneWayCallGreeter(), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      ackMessage(1),
      completionMessage(3, greetResponse("FRANCESCO")),
    ]).run();

    expect(result).toStrictEqual([
      sideEffectMessage("abcd"),
      backgroundInvokeMessage(
        "test.TestGreeter",
        "Greet",
        greetRequest("abcd")
      ),
      invokeMessage("test.TestGreeter", "Greet", greetRequest("abcd")),
      outputMessage(greetResponse("Hello FRANCESCO")),
      END_MESSAGE,
    ]);
  });

  it("handles replay and then invoke", async () => {
    const result = await new TestDriver(new SideEffectAndOneWayCallGreeter(), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      sideEffectMessage("abcd"),
      completionMessage(3, greetResponse("FRANCESCO")),
    ]).run();

    expect(result).toStrictEqual([
      backgroundInvokeMessage(
        "test.TestGreeter",
        "Greet",
        greetRequest("abcd")
      ),
      invokeMessage("test.TestGreeter", "Greet", greetRequest("abcd")),
      outputMessage(greetResponse("Hello FRANCESCO")),
      END_MESSAGE,
    ]);
  });
});

class NumericSideEffectGreeter implements TestGreeter {
  constructor(readonly sideEffectOutput: number) {}

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async greet(request: TestRequest): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    // state
    const response = await ctx.sideEffect(async () => {
      return this.sideEffectOutput;
    });

    return TestResponse.create({ greeting: `Hello ${response}` });
  }
}

describe("NumericSideEffectGreeter", () => {
  it("handles acks", async () => {
    const result = await new TestDriver(new NumericSideEffectGreeter(123), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      ackMessage(1),
    ]).run();

    expect(result).toStrictEqual([
      sideEffectMessage(123),
      outputMessage(greetResponse("Hello 123")),
      END_MESSAGE,
    ]);
  });

  it("handles replay with value", async () => {
    const result = await new TestDriver(new NumericSideEffectGreeter(123), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      sideEffectMessage(123),
    ]).run();

    expect(result).toStrictEqual([
      outputMessage(greetResponse("Hello 123")),
      END_MESSAGE,
    ]);
  });
});

enum OrderStatus {
  ORDERED,
  DELIVERED,
}

class EnumSideEffectGreeter implements TestGreeter {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async greet(request: TestRequest): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    // state
    const response = await ctx.sideEffect(async () => {
      return OrderStatus.ORDERED;
    });

    return TestResponse.create({ greeting: `Hello ${response}` });
  }
}

describe("EnumSideEffectGreeter", () => {
  it("handles acks with value enum", async () => {
    const result = await new TestDriver(new EnumSideEffectGreeter(), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      ackMessage(1),
    ]).run();

    expect(result).toStrictEqual([
      sideEffectMessage(OrderStatus.ORDERED),
      outputMessage(greetResponse("Hello 0")),
      END_MESSAGE,
    ]);
  });

  it("handles replay with value enum", async () => {
    const result = await new TestDriver(new EnumSideEffectGreeter(), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      sideEffectMessage(OrderStatus.ORDERED),
    ]).run();

    expect(result).toStrictEqual([
      outputMessage(greetResponse("Hello 0")),
      END_MESSAGE,
    ]);
  });
});

class FailingSideEffectGreeter implements TestGreeter {
  constructor(private readonly terminalError: boolean) {}

  async greet(): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    // state
    const response = await ctx.sideEffect(async () => {
      throw this.terminalError
        ? new TerminalError("Failing user code")
        : new Error("Failing user code");
    });

    return TestResponse.create({ greeting: `Hello ${response}` });
  }
}

describe("Side effects error-handling", () => {
  it("handles terminal errors by producing error output", async () => {
    const result = await new TestDriver(new FailingSideEffectGreeter(true), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      ackMessage(1),
    ]).run();

    // When the user code fails we do want to see a side effect message with a failure
    // For invalid user code, we do not want to see this.
    expect(result.length).toStrictEqual(3);
    checkTerminalError(result[1], "Failing user code");
    expect(result[2]).toStrictEqual(END_MESSAGE);
  });

  it("handles non-terminal errors by retrying (with sleep and suspension)", async () => {
    const result = await new TestDriver(new FailingSideEffectGreeter(false), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      completionMessage(1),
    ]).run();

    // When the user code fails we do want to see a side effect message with a failure
    // For invalid user code, we do not want to see this.
    expect(result.length).toBeGreaterThanOrEqual(1);
    expect(result[0]).toStrictEqual(
      sideEffectMessage(
        undefined,
        failureWithTerminal(false, "Failing user code")
      )
    );
  });
});

class FailingGetSideEffectGreeter implements TestGreeter {
  constructor(readonly sideEffectOutput: number) {}

  async greet(): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    // state
    const response = await ctx.sideEffect(async () => {
      await ctx.get("state");
      return this.sideEffectOutput;
    });

    return TestResponse.create({ greeting: `Hello ${response}` });
  }
}

describe("FailingGetSideEffectGreeter", () => {
  it("fails on invalid operation getState in sideEffect", async () => {
    const result = await new TestDriver(new FailingGetSideEffectGreeter(123), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      ackMessage(1),
    ]).run();

    expect(result.length).toStrictEqual(3);
    checkTerminalError(
      result[1],
      "You cannot do get state calls from within a side effect."
    );
    expect(result[2]).toStrictEqual(END_MESSAGE);
  });
});

class FailingSetSideEffectGreeter implements TestGreeter {
  constructor(readonly sideEffectOutput: number) {}

  async greet(): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    // state
    const response = await ctx.sideEffect(async () => {
      ctx.set("state", 13);
      return this.sideEffectOutput;
    });

    return TestResponse.create({ greeting: `Hello ${response}` });
  }
}

describe("FailingSetSideEffectGreeter", () => {
  it("fails on invalid operation setState in sideEffect", async () => {
    const result = await new TestDriver(new FailingSetSideEffectGreeter(123), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      ackMessage(1),
    ]).run();

    expect(result.length).toStrictEqual(3);
    checkTerminalError(
      result[1],
      "You cannot do set state calls from within a side effect."
    );
    expect(result[2]).toStrictEqual(END_MESSAGE);
  });
});

class FailingClearSideEffectGreeter implements TestGreeter {
  constructor(readonly sideEffectOutput: number) {}

  async greet(): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    // state
    const response = await ctx.sideEffect(async () => {
      ctx.clear("state");
      return this.sideEffectOutput;
    });

    return TestResponse.create({ greeting: `Hello ${response}` });
  }
}

describe("FailingClearSideEffectGreeter", () => {
  it("fails on invalid operation clearState in sideEffect", async () => {
    const result = await new TestDriver(
      new FailingClearSideEffectGreeter(123),
      [startMessage(), inputMessage(greetRequest("Till")), ackMessage(1)]
    ).run();

    expect(result.length).toStrictEqual(3);
    checkTerminalError(
      result[1],
      "You cannot do clear state calls from within a side effect"
    );
    expect(result[2]).toStrictEqual(END_MESSAGE);
  });
});

class FailingNestedSideEffectGreeter implements TestGreeter {
  constructor(readonly sideEffectOutput: number) {}

  async greet(): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    // state
    const response = await ctx.sideEffect(async () => {
      await ctx.sideEffect(async () => {
        return this.sideEffectOutput;
      });
    });

    return TestResponse.create({ greeting: `Hello ${response}` });
  }
}

describe("FailingNestedSideEffectGreeter", () => {
  it("fails on invalid operation sideEffect in sideEffect", async () => {
    const result = await new TestDriver(
      new FailingNestedSideEffectGreeter(123),
      [startMessage(), inputMessage(greetRequest("Till")), ackMessage(1)]
    ).run();

    expect(result.length).toStrictEqual(3);
    checkTerminalError(
      result[1],
      "You cannot do sideEffect calls from within a side effect."
    );
    expect(result[2]).toStrictEqual(END_MESSAGE);
  });

  it("fails on invalid replayed operation sideEffect in sideEffect", async () => {
    const result = await new TestDriver(
      new FailingNestedSideEffectGreeter(123),
      [
        startMessage(),
        inputMessage(greetRequest("Till")),
        sideEffectMessage(
          undefined,
          failureWithTerminal(
            true,
            "Error: You cannot do sideEffect state calls from within a side effect.",
            ErrorCodes.INTERNAL
          )
        ),
      ]
    ).run();

    expect(result.length).toStrictEqual(2);
    checkTerminalError(
      result[0],
      "You cannot do sideEffect state calls from within a side effect"
    );
    expect(result[1]).toStrictEqual(END_MESSAGE);
  });
});

class FailingNestedWithoutAwaitSideEffectGreeter implements TestGreeter {
  constructor(readonly sideEffectOutput: number) {}

  async greet(): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    // state
    const response = await ctx.sideEffect(async () => {
      // without awaiting
      ctx.sideEffect(async () => {
        return this.sideEffectOutput;
      });
    });

    return TestResponse.create({ greeting: `Hello ${response}` });
  }
}

// //TODO This test causes one of the later tests to fail
// // it seems there is something not cleaned up correctly...
// // describe("FailingNestedWithoutAwaitSideEffectGreeter: invalid nested side effect in side effect with ack", () => {
// //   it("should call greet", async () => {
// //     const result = await new TestDriver(
// //       new FailingNestedWithoutAwaitSideEffectGreeter(123),
// //       [startMessage(), inputMessage(greetRequest("Till"))]
// //     ).run();
// //
// //     expect(result.length).toStrictEqual(1);
// //     checkError(
// //       result[0],
// //       "You cannot do sideEffect calls from within a side effect."
// //     );
// //   });
// // });

describe("FailingNestedWithoutAwaitSideEffectGreeter", () => {
  it("fails on invalid operation unawaited side effect in sideEffect", async () => {
    const result = await new TestDriver(
      new FailingNestedWithoutAwaitSideEffectGreeter(123),
      [
        startMessage(),
        inputMessage(greetRequest("Till")),
        sideEffectMessage(
          undefined,
          failureWithTerminal(
            true,
            "Error: You cannot do sideEffect state calls from within a side effect.",
            ErrorCodes.INTERNAL
          )
        ),
      ]
    ).run();

    expect(result.length).toStrictEqual(2);
    checkTerminalError(
      result[0],
      "You cannot do sideEffect state calls from within a side effect"
    );
    expect(result[1]).toStrictEqual(END_MESSAGE);
  });
});

class FailingOneWayCallInSideEffectGreeter implements TestGreeter {
  constructor(readonly sideEffectOutput: number) {}

  async greet(): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    // state
    const response = await ctx.sideEffect(async () => {
      await ctx.oneWayCall(async () => {
        return;
      });
    });

    return TestResponse.create({ greeting: `Hello ${response}` });
  }
}

describe("FailingOneWayCallInSideEffectGreeter", () => {
  it("fails on invalid operation oneWayCall in sideEffect", async () => {
    const result = await new TestDriver(
      new FailingOneWayCallInSideEffectGreeter(123),
      [startMessage(), inputMessage(greetRequest("Till")), ackMessage(1)]
    ).run();

    expect(result.length).toStrictEqual(3);
    checkTerminalError(
      result[1],
      "You cannot do oneWayCall calls from within a side effect"
    );
    expect(result[2]).toStrictEqual(END_MESSAGE);
  });
});

class FailingResolveAwakeableSideEffectGreeter implements TestGreeter {
  constructor(readonly sideEffectOutput: number) {}

  async greet(): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    // state
    const response = await ctx.sideEffect(async () => {
      const awakeableIdentifier = getAwakeableId(1);
      ctx.resolveAwakeable(awakeableIdentifier, "hello");
    });

    return TestResponse.create({ greeting: `Hello ${response}` });
  }
}

describe("FailingResolveAwakeableSideEffectGreeter", () => {
  it("fails on invalid operation resolveAwakeable in sideEffect", async () => {
    const result = await new TestDriver(
      new FailingResolveAwakeableSideEffectGreeter(123),
      [startMessage(), inputMessage(greetRequest("Till")), ackMessage(1)]
    ).run();

    expect(result.length).toStrictEqual(3);
    checkTerminalError(
      result[1],
      "You cannot do resolveAwakeable calls from within a side effect."
    );
    expect(result[2]).toStrictEqual(END_MESSAGE);
  });
});

class FailingRejectAwakeableSideEffectGreeter implements TestGreeter {
  constructor(readonly sideEffectOutput: number) {}

  async greet(): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    // state
    const response = await ctx.sideEffect(async () => {
      const awakeableIdentifier = getAwakeableId(1);
      ctx.rejectAwakeable(awakeableIdentifier, "hello");
    });

    return TestResponse.create({ greeting: `Hello ${response}` });
  }
}

describe("FailingRejectAwakeableSideEffectGreeter", () => {
  it("fails on invalid operation rejectAwakeable in sideEffect", async () => {
    const result = await new TestDriver(
      new FailingRejectAwakeableSideEffectGreeter(123),
      [startMessage(), inputMessage(greetRequest("Till")), ackMessage(1)]
    ).run();

    expect(result.length).toStrictEqual(3);
    checkTerminalError(
      result[1],
      "You cannot do rejectAwakeable calls from within a side effect."
    );
    expect(result[2]).toStrictEqual(END_MESSAGE);
  });
});

class FailingSleepSideEffectGreeter implements TestGreeter {
  constructor(readonly sideEffectOutput: number) {}

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async greet(request: TestRequest): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    // state
    const response = await ctx.sideEffect(async () => {
      await ctx.sleep(1000);
    });

    return TestResponse.create({ greeting: `Hello ${response}` });
  }
}

describe("FailingSleepSideEffectGreeter", () => {
  it("fails on invalid operation sleep in sideEffect", async () => {
    const result = await new TestDriver(
      new FailingSleepSideEffectGreeter(123),
      [startMessage(), inputMessage(greetRequest("Till")), ackMessage(1)]
    ).run();

    expect(result.length).toStrictEqual(3);
    checkTerminalError(
      result[1],
      "You cannot do sleep calls from within a side effect."
    );
    expect(result[2]).toStrictEqual(END_MESSAGE);
  });
});

class FailingAwakeableSideEffectGreeter implements TestGreeter {
  constructor(readonly sideEffectOutput: number) {}

  async greet(): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    // state
    const response = await ctx.sideEffect(async () => {
      ctx.awakeable<string>();
    });

    return TestResponse.create({ greeting: `Hello ${response}` });
  }
}

describe("FailingAwakeableSideEffectGreeter", () => {
  it("fails on invalid operation awakeable in sideEffect", async () => {
    const result = await new TestDriver(
      new FailingAwakeableSideEffectGreeter(123),
      [startMessage(), inputMessage(greetRequest("Till")), ackMessage(1)]
    ).run();

    expect(result.length).toStrictEqual(3);
    checkTerminalError(
      result[1],
      "You cannot do awakeable calls from within a side effect."
    );
    expect(result[2]).toStrictEqual(END_MESSAGE);
  });
});

export class AwaitSideEffectService implements TestGreeter {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async greet(request: TestRequest): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    let invocationCount = 0;

    await ctx.sideEffect<void>(async () => {
      invocationCount++;
    });
    await ctx.sideEffect<void>(async () => {
      invocationCount++;
    });
    await ctx.sideEffect<void>(async () => {
      invocationCount++;
    });

    return { greeting: invocationCount.toString() };
  }
}

describe("AwaitSideEffectService", () => {
  it("handles acks of all side effects", async () => {
    const result = await new TestDriver(new AwaitSideEffectService(), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      ackMessage(1),
      ackMessage(2),
      ackMessage(3),
    ]).run();

    expect(result).toStrictEqual([
      sideEffectMessage(),
      sideEffectMessage(),
      sideEffectMessage(),
      outputMessage(greetResponse("3")),
      END_MESSAGE,
    ]);
  });

  it("handles replay of first side effect and acks of the others", async () => {
    const result = await new TestDriver(new AwaitSideEffectService(), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      sideEffectMessage(),
      ackMessage(2),
      ackMessage(3),
    ]).run();

    expect(result).toStrictEqual([
      sideEffectMessage(),
      sideEffectMessage(),
      outputMessage(greetResponse("2")),
      END_MESSAGE,
    ]);
  });

  it("handles replay of first two side effect and ack of the other", async () => {
    const result = await new TestDriver(new AwaitSideEffectService(), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      sideEffectMessage(),
      sideEffectMessage(),
      ackMessage(3),
    ]).run();

    expect(result).toStrictEqual([
      sideEffectMessage(),
      outputMessage(greetResponse("1")),
      END_MESSAGE,
    ]);
  });

  it("handles replay of all side effects", async () => {
    const result = await new TestDriver(new AwaitSideEffectService(), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      sideEffectMessage(),
      sideEffectMessage(),
      sideEffectMessage(),
    ]).run();

    expect(result).toStrictEqual([
      outputMessage(greetResponse("0")),
      END_MESSAGE,
    ]);
  });
});

export class SequentialSideEffectAwaitingLaterService implements TestGreeter {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async greet(request: TestRequest): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    const latch = new Latch();
    const a1 = ctx.sideEffect<number>(async () => {
      // Wait on a2 closure to be invoked
      await latch.await();
      return 1;
    });
    const a2 = ctx.sideEffect<number>(async () => {
      return 2;
    });

    // Unblock a1
    latch.resolveAtNextTick(undefined);

    await a1;
    await a2;

    return TestResponse.create();
  }
}

describe("SequentialSideEffectAwaitingLaterService", () => {
  it("sequential side effects should create correct order of entries", async () => {
    const result = await new TestDriver(
      new SequentialSideEffectAwaitingLaterService(),
      [
        startMessage(),
        inputMessage(greetRequest("Till")),
        ackMessage(1),
        ackMessage(2),
      ]
    ).run();

    expect(result).toStrictEqual([
      sideEffectMessage(1),
      sideEffectMessage(2),
      outputMessage(greetResponse()),
      END_MESSAGE,
    ]);
  });
});
//
export class SequentialSideEffectWithRetryAwaitingLaterService
  implements TestGreeter
{
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async greet(request: TestRequest): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    let retriedOnce = false;
    const latch = new Latch();
    const a1 = ctx.sideEffect<number>(
      async () => {
        // Wait on a2 closure to be invoked
        await latch.await();

        if (!retriedOnce) {
          retriedOnce = true;
          throw new Error("error");
        }
        return 1;
      },
      { maxRetries: 2, policy: FIXED_DELAY, maxDelayMs: 1 }
    );
    const a2 = ctx.sideEffect<number>(async () => {
      return 2;
    });

    // Unblock a1
    latch.resolveAtNextTick(undefined);

    await a1;
    await a2;

    return TestResponse.create();
  }
}

describe("SequentialSideEffectWithRetryAwaitingLaterService", () => {
  it("sequential side effects should create correct order of entries", async () => {
    const result = await new TestDriver(
      new SequentialSideEffectWithRetryAwaitingLaterService(),
      [
        startMessage(),
        inputMessage(greetRequest("Till")),
        ackMessage(1),
        completionMessage(2, undefined, true),
        ackMessage(3),
        ackMessage(4),
      ]
    ).run();

    expect(result[0]).toStrictEqual(
      sideEffectMessage(new TerminalError("error"))
    );
    expect(result[1].messageType).toStrictEqual(SLEEP_ENTRY_MESSAGE_TYPE);
    expect(result.slice(2)).toStrictEqual([
      sideEffectMessage(1),
      sideEffectMessage(2),
      outputMessage(greetResponse()),
      END_MESSAGE,
    ]);
  });
});

export class InterleaveSideEffectAwaitingLaterWithSetStateService
  implements TestGreeter
{
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async greet(request: TestRequest): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    const latch = new Latch();
    const a1 = ctx.sideEffect<number>(async () => {
      // Wait on a2 closure to be invoked
      await latch.await();
      return 1;
    });
    ctx.set("myState", "myValue");
    const a2 = ctx.sideEffect<number>(async () => {
      return 2;
    });

    // Unblock a1
    latch.resolveAtNextTick(undefined);

    await a1;
    await a2;

    return TestResponse.create();
  }
}

describe("InterleaveSideEffectAwaitingLaterWithSetStateService", () => {
  it("sequential side effects should create correct order of entries", async () => {
    const result = await new TestDriver(
      new InterleaveSideEffectAwaitingLaterWithSetStateService(),
      [
        startMessage(),
        inputMessage(greetRequest("Till")),
        ackMessage(1),
        ackMessage(3),
      ]
    ).run();

    expect(result).toStrictEqual([
      sideEffectMessage(1),
      setStateMessage("myState", "myValue"),
      sideEffectMessage(2),
      outputMessage(greetResponse()),
      END_MESSAGE,
    ]);
  });
});

export class TerminalErrorSideEffectService implements TestGreeter {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  async greet(request: TestRequest): Promise<TestResponse> {
    const ctx = restate.useContext(this);

    await ctx.sideEffect<void>(async () => {
      throw new TerminalError("Something bad happened.");
    });

    return { greeting: `Hello ${request.name}` };
  }
}

describe("TerminalErrorSideEffectService", () => {
  it("handles terminal error", async () => {
    const result = await new TestDriver(new TerminalErrorSideEffectService(), [
      startMessage(),
      inputMessage(greetRequest("Till")),
      ackMessage(1),
    ]).run();

    expect(result[0]).toStrictEqual(
      sideEffectMessage(
        undefined,
        failureWithTerminal(
          true,
          "Something bad happened.",
          ErrorCodes.INTERNAL
        )
      )
    );
    checkTerminalError(result[1], "Something bad happened.");
  });
});
