/* eslint-disable */
/**
 * Generated `api` utility.
 *
 * THIS CODE IS AUTOMATICALLY GENERATED.
 *
 * To regenerate, run `npx convex dev`.
 * @module
 */

import type * as atomicMutators from "../atomicMutators.js";
import type * as client from "../client.js";
import type * as documents from "../documents.js";
import type * as types from "../types.js";

import type {
  ApiFromModules,
  FilterApi,
  FunctionReference,
} from "convex/server";
/**
 * A utility for referencing Convex functions in your app's API.
 *
 * Usage:
 * ```js
 * const myFunctionReference = api.myModule.myFunction;
 * ```
 */
declare const fullApi: ApiFromModules<{
  atomicMutators: typeof atomicMutators;
  client: typeof client;
  documents: typeof documents;
  types: typeof types;
}>;
declare const fullApiWithMounts: typeof fullApi & {
  documents: {
    deleteDoc: FunctionReference<
      "mutation",
      "public",
      { atomicDelete: string; id: string; triggers: Array<string> },
      null
    >;
    insert: FunctionReference<
      "mutation",
      "public",
      { atomicInsert: string; triggers: Array<string>; value: any },
      string
    >;
    patch: FunctionReference<
      "mutation",
      "public",
      { atomicPatch: string; id: string; triggers: Array<string>; value: any },
      null
    >;
    replace: FunctionReference<
      "mutation",
      "public",
      {
        atomicReplace: string;
        id: string;
        triggers: Array<string>;
        value: any;
      },
      null
    >;
  };
};

export declare const api: FilterApi<
  typeof fullApiWithMounts,
  FunctionReference<any, "public">
>;
export declare const internal: FilterApi<
  typeof fullApiWithMounts,
  FunctionReference<any, "internal">
>;
