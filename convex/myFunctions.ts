import { v } from "convex/values";
import { BTree, mutationWithBTree } from "../btree/withBTree";
import {
  customMutation,
} from "convex-helpers/server/customFunctions";
import { mutation, query, app, QueryCtx } from "./_generated/server";
import { DataModel } from "./_generated/dataModel";

const mutationWithNumbers = customMutation(
  mutation as any,
  mutationWithBTree<DataModel, "numbers", number>({
    tableName: "numbers",
    api: app.numbersBTree,
    getKey: (doc) => doc.value,
    getSummand: (doc) => doc.value,
  })
);

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
