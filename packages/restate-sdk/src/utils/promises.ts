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

/**
 * Transform the result of a promise, without invoking the then method.
 */
export function transformIt<T, U = T>(
  promise: Promise<T>,
  transformOnfulfilled: (value: T) => U | PromiseLike<U>
): Promise<U> {
  return {
    then: function <TResult1 = U, TResult2 = never>(
      onfulfilled?:
        | ((value: U) => TResult1 | PromiseLike<TResult1>)
        | null
        | undefined,
      onrejected?:
        | ((reason: any) => TResult2 | PromiseLike<TResult2>)
        | null
        | undefined
    ): Promise<TResult1 | TResult2> {
      if (onfulfilled) {
        return promise.then(
          async (t) => onfulfilled(await transformOnfulfilled(t)),
          onrejected
        );
      } else {
        return promise.then(undefined, onrejected);
      }
    },
    catch: promise.catch.bind(promise),
    finally: function (
      onfinally?: (() => void) | null | undefined
    ): Promise<U> {
      return promise.then(transformOnfulfilled).finally(onfinally);
    },
    [Symbol.toStringTag]: "",
  };
}

/**
 * Add a listener when the {@link then} method is invoked.
 */
export function listenOnThen<T>(
  promise: Promise<T>,
  onThen?: () => void
): Promise<T> {
  if (onThen) {
    return {
      then: function <TResult1 = T, TResult2 = never>(
        onfulfilled?:
          | ((value: T) => TResult1 | PromiseLike<TResult1>)
          | null
          | undefined,
        onrejected?:
          | ((reason: any) => TResult2 | PromiseLike<TResult2>)
          | null
          | undefined
      ): Promise<TResult1 | TResult2> {
        if (onThen !== undefined) {
          onThen();
        }
        return promise.then(onfulfilled, onrejected);
      },
      catch: function <TResult = never>(
        onrejected?:
          | ((reason: any) => TResult | PromiseLike<TResult>)
          | null
          | undefined
      ): Promise<T | TResult> {
        return listenOnThen(promise.catch(onrejected), onThen);
      },
      finally: function (
        onfinally?: (() => void) | null | undefined
      ): Promise<T> {
        return listenOnThen(promise.finally(onfinally), onThen);
      },
      [Symbol.toStringTag]: "",
    };
  } else {
    return promise;
  }
}

// Like https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Promise/withResolvers
// (not yet available in node)
export class CompletablePromise<T> {
  private success!: (value: T | PromiseLike<T>) => void;
  private failure!: (reason?: any) => void;

  public readonly promise: Promise<T>;

  constructor() {
    this.promise = new Promise((resolve, reject) => {
      this.success = resolve;
      this.failure = reject;
    });
  }

  public resolve(value: T) {
    this.success(value);
  }

  public reject(reason?: any) {
    this.failure(reason);
  }
}
