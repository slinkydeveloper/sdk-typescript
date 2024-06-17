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

import { rlog } from "../../logger";
import { LambdaConnection } from "../../connection/lambda_connection";
import { InvocationBuilder } from "../../invocation";
import { decodeMessagesBuffer } from "../../io/decoder";
import { Message } from "../../types/types";
import { StateMachine } from "../../state_machine";
import { ensureError } from "../../types/errors";
import {
  OUTPUT_ENTRY_MESSAGE_TYPE,
  isServiceProtocolVersionSupported,
  parseServiceProtocolVersion,
  selectSupportedServiceDiscoveryProtocolVersion,
  serviceDiscoveryProtocolVersionToHeaderValue,
  serviceProtocolVersionToHeaderValue,
} from "../../types/protocol";
import { ProtocolMode } from "../../types/discovery";
import { ComponentHandler, parseUrlComponents } from "../../types/components";
import { validateRequestSignature } from "../request_signing/validate";
import { X_RESTATE_SERVER } from "../../user_agent";
import { Buffer } from "node:buffer";
import { ServiceDiscoveryProtocolVersion } from "../../generated/proto/discovery_pb";
import { ServiceProtocolVersion } from "../../generated/proto/protocol_pb";
import { EndpointBuilder } from "../endpoint_builder";

export interface Headers {
  [name: string]: string | string[] | undefined;
}

export interface ResponseHeaders {
  [name: string]: string;
}

export interface AdditionalContext {
  [name: string]: string;
}

export interface RestateRequest {
  readonly url: string;
  readonly headers: Headers;
  readonly body: Uint8Array;
}

export interface RestateResponse {
  readonly headers: ResponseHeaders;
  readonly statusCode: number;
  readonly body: Uint8Array;
}

export interface RestateHandler {
  handle(
    request: RestateRequest,
    context?: AdditionalContext
  ): Promise<RestateResponse>;
}

export class GenericHandler implements RestateHandler {
  constructor(private readonly endpoint: EndpointBuilder) {}

  // --------------------------------------------------------------------------

  public async handle(
    request: RestateRequest,
    context?: AdditionalContext
  ): Promise<RestateResponse> {
    const path = request.url;

    const error = await this.validateConnectionSignature(path, request.headers);
    if (error !== null) {
      return error;
    }

    const parsed = parseUrlComponents(path);
    if (!parsed) {
      const msg = `Invalid path: path doesn't end in /invoke/SvcName/handlerName and also not in /discover: ${path}`;
      rlog.trace(msg);
      return this.toErrorResponse(404, msg);
    }
    if (parsed === "discovery") {
      return this.handleDiscovery(request.headers["accept"]);
    }
    const serviceProtocolVersionString = request.headers["content-type"];
    if (typeof serviceProtocolVersionString !== "string") {
      const errorMessage = "Missing content-type header";
      rlog.warn(errorMessage);
      return this.toErrorResponse(415, errorMessage);
    }
    const serviceProtocolVersion = parseServiceProtocolVersion(
      serviceProtocolVersionString
    );
    if (!isServiceProtocolVersionSupported(serviceProtocolVersion)) {
      const errorMessage = `Unsupported service protocol version '${serviceProtocolVersionString}'`;
      rlog.warn(errorMessage);
      return this.toErrorResponse(415, errorMessage);
    }
    const method = this.endpoint.componentByName(parsed.componentName);
    if (!method) {
      const msg = `No service found for URL: ${parsed}`;
      rlog.error(msg);
      return this.toErrorResponse(404, msg);
    }
    const handler = method?.handlerMatching(parsed);
    if (!handler) {
      const msg = `No service found for URL: ${parsed}`;
      rlog.error(msg);
      return this.toErrorResponse(404, msg);
    }
    if (!request.body) {
      const msg = "The incoming message body was null";
      rlog.error(msg);
      return this.toErrorResponse(400, msg);
    }
    return this.handleInvoke(
      handler,
      request.body,
      request.headers,
      serviceProtocolVersion,
      context ?? {}
    );
  }

  private async validateConnectionSignature(
    path: string,
    headers: Headers
  ): Promise<RestateResponse | null> {
    if (!this.endpoint.keySet) {
      // not validating
      return null;
    }

    try {
      const validateResponse = await validateRequestSignature(
        this.endpoint.keySet,
        path,
        headers
      );

      if (!validateResponse.valid) {
        rlog.error(
          `Rejecting request as its JWT did not validate: ${validateResponse.error}`
        );
        return this.toErrorResponse(401, "Unauthorized");
      } else {
        return null;
      }
    } catch (e) {
      const error = ensureError(e);
      rlog.error(
        "Error while attempting to validate request signature: " +
          (error.stack ?? error.message)
      );
      return this.toErrorResponse(401, "Unauthorized");
    }
  }

  private async handleInvoke(
    handler: ComponentHandler,
    body: Uint8Array,
    headers: Record<string, string | string[] | undefined>,
    serviceProtocolVersion: ServiceProtocolVersion,
    context: AdditionalContext
  ): Promise<RestateResponse> {
    try {
      // build the previous journal from the events
      let decodedEntries: Message[] | null = decodeMessagesBuffer(
        Buffer.from(body)
      );
      const journalBuilder = new InvocationBuilder(handler);
      decodedEntries.forEach((e: Message) => journalBuilder.handleMessage(e));
      const alreadyCompleted =
        decodedEntries.find(
          (e: Message) => e.messageType === OUTPUT_ENTRY_MESSAGE_TYPE
        ) !== undefined;
      decodedEntries = null;

      // set up and invoke the state machine
      const connection = new LambdaConnection(headers, alreadyCompleted);
      const invocation = journalBuilder.build();
      const stateMachine = new StateMachine(
        connection,
        invocation,
        ProtocolMode.REQUEST_RESPONSE,
        handler.kind(),
        invocation.inferLoggerContext(context)
      );
      await stateMachine.invoke();
      const result = await connection.getResult();

      return {
        headers: {
          "content-type": serviceProtocolVersionToHeaderValue(
            serviceProtocolVersion
          ),
          "x-restate-server": X_RESTATE_SERVER,
        },
        statusCode: 200,
        body: result,
      };
    } catch (e) {
      const error = ensureError(e);
      rlog.error(error.message);
      rlog.error(error.stack);
      return this.toErrorResponse(500, error.message);
    }
  }

  private handleDiscovery(
    acceptVersionsString: string | string[] | undefined
  ): RestateResponse {
    if (typeof acceptVersionsString !== "string") {
      const errorMessage = "Missing accept header";
      rlog.warn(errorMessage);
      return this.toErrorResponse(415, errorMessage);
    }

    const serviceDiscoveryProtocolVersion =
      selectSupportedServiceDiscoveryProtocolVersion(acceptVersionsString);

    if (
      serviceDiscoveryProtocolVersion ===
      ServiceDiscoveryProtocolVersion.SERVICE_DISCOVERY_PROTOCOL_VERSION_UNSPECIFIED
    ) {
      const errorMessage = `Unsupported service discovery protocol version '${acceptVersionsString}'`;
      rlog.warn(errorMessage);
      return this.toErrorResponse(415, errorMessage);
    }

    const discovery = this.endpoint.computeDiscovery(
      ProtocolMode.REQUEST_RESPONSE
    );

    let body;

    if (
      serviceDiscoveryProtocolVersion === ServiceDiscoveryProtocolVersion.V1
    ) {
      const discoveryJson = JSON.stringify(discovery);
      body = Buffer.from(discoveryJson);
    } else {
      // should not be reached since we check for compatibility before
      throw new Error(
        `Unsupported service discovery protocol version: ${serviceDiscoveryProtocolVersion}`
      );
    }

    return {
      headers: {
        "content-type": serviceDiscoveryProtocolVersionToHeaderValue(
          serviceDiscoveryProtocolVersion
        ),
        "x-restate-server": X_RESTATE_SERVER,
      },
      statusCode: 200,
      body,
    };
  }

  private toErrorResponse(code: number, message: string): RestateResponse {
    return {
      headers: {
        "content-type": "text/plain",
        "x-restate-server": X_RESTATE_SERVER,
      },
      statusCode: code,
      body: Buffer.from(JSON.stringify({ message })),
    };
  }
}