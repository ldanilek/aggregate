import { v, Validator } from "convex/values";
import { BTree, initBTree } from "../btree/withBTree";
import {
  customMutation,
} from "convex-helpers/server/customFunctions";
import { mutation, query, app, QueryCtx, internalMutation, internalQuery } from "./_generated/server";
import { DataModel, Doc } from "./_generated/dataModel";
import { FunctionReference } from "convex/server";
import { internal } from "./_generated/api";
import { WithTriggers, withTriggers } from "../triggers/client";
import { atomicMutators } from "../triggers/atomicMutators";
import { TriggerArgs } from "../triggers/types";

const withAllTriggers: WithTriggers<DataModel> = withTriggers(app.triggers, {
  numbers: {
    atomicMutators: internal.myFunctions,
    triggers: [app.numbersBTree.btree.trigger as FunctionReference<"mutation", any, TriggerArgs<DataModel, "numbers">, null>],
  },
});

export const { atomicInsert, atomicPatch, atomicReplace, atomicDelete } = atomicMutators("numbers");

const mutationWithNumbers = customMutation(
  mutation,
  withAllTriggers,
);

export const getKey = internalQuery({
  args: { doc: v.any() as Validator<Doc<"numbers">> },
  returns: v.object({ key: v.number(), summand: v.optional(v.number()) }),
  handler: async (_ctx, { doc }) => {
    return { key: doc.value, summand: doc.value };
  }
});

export const initNumbersBTree = internalMutation({
  args: {},
  handler: async (ctx) => {
    await initBTree(ctx, app.numbersBTree, internal.myFunctions.getKey);
  },
});

function numbersBTree(ctx: QueryCtx) {
  return new BTree<DataModel, "numbers", number>(
    ctx,
    app.numbersBTree
  );
}

// Write your Convex functions in any file inside this directory (`convex`).
// See https://docs.convex.dev/functions for more.

export const listNumbers = query({
  args: {
    count: v.number(),
  },
  handler: async (ctx, args) => {
    const numbers = await ctx.db
      .query("numbers")
      // Ordered by _creationTime, return most recent
      .order("desc")
      .take(args.count);
    return numbers.reverse().map((number) => number.value);
  },
});

// You can write data to the database via a mutation:
export const addNumber = mutationWithNumbers({
  // Validators for arguments.
  args: {
    value: v.number(),
  },

  // Mutation implementation.
  handler: async (ctx, args) => {
    //// Insert or modify documents in the database here.
    //// Mutations can also read from the database like queries.
    //// See https://docs.convex.dev/database/writing-data.

    const tree = numbersBTree(ctx);
    const exists = (await tree.get(args.value)) !== null;
    if (exists) {
      console.log("skipped adding duplicate", args.value);
    }

    const id = await ctx.db.insert("numbers", { value: args.value });

    console.log("Added new document with id:", id);
    // Optionally, return a value from your mutation.
    // return id;
  },
});

export const removeNumber = mutationWithNumbers({
  args: { number: v.id("numbers") },
  handler: async (ctx, args) => {
    await ctx.db.delete("numbers", args.number);
  },
});

export const numberAtIndex = query({
  args: { index: v.number() },
  handler: async (ctx, args) => {
    return numbersBTree(ctx).at(args.index);
  },
});

export const countNumbers = query({
  handler: async (ctx) => {
    return await numbersBTree(ctx).count();
  },
});

export const sumNumbers = query({
  handler: async (ctx) => {
    return await numbersBTree(ctx).sum();
  },
});

export const backfillBTree = mutationWithNumbers({
  args: {},
  handler: async (ctx) => {
    await ctx.runMutation(app.numbersBTree.btree.clearTree, { });

    for await (const doc of ctx.db.query("numbers")) {
      await ctx.db.patch("numbers", doc._id, {});
      console.log("backfilled", doc.value);
      await numbersBTree(ctx).validate();
    }
  },
});

export const validateBTree = query({
  args: {},
  handler: async (ctx) => {
    await numbersBTree(ctx).validate();
  },
});
