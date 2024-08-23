/* eslint-disable */
/**
 * Generated `api` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * Generated by convex@1.13.3-alpha.0.
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type * as atomicMutators from "../atomicMutators.js";
import type * as client from "../client.js";
import type * as documents from "../documents.js";
import type * as types from "../types.js";

import type { ApiFromModules, FunctionReference } from "convex/server";
/**
 * A utility for referencing Convex functions in your app's API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = functions.myModule.myFunction;
 * ```
 */
declare const functions: ApiFromModules<{
  atomicMutators: typeof atomicMutators;
  client: typeof client;
  documents: typeof documents;
  types: typeof types;
}>;