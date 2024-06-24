import { v } from "convex/values";
import { mutationWithBTree, queryWithBTree } from "./btree/withBTree";
import {
  customMutation,
  customQuery,
} from "convex-helpers/server/customFunctions";
import { mutation, query } from "./_generated/server";

const mutationWithNumbers = customMutation(
  mutation,
  mutationWithBTree({
    tableName: "numbers",
    btreeName: "numbers",
    getKey: (doc) => doc.value,
  })
);

const queryWithNumbers = customQuery(
  query,
  queryWithBTree({
    tableName: "numbers",
    btreeName: "numbers",
    getKey: (doc) => doc.value,
  })
);

// Write your Convex functions in any file inside this directory (`convex`).
// See https://docs.convex.dev/functions for more.

export const listNumbers = queryWithNumbers({
  args: {
    count: v.number(),
  },
  handler: async (ctx, args) => {
    const numbers = await ctx.db
      .query("numbers")
      // Ordered by _creationTime, return most recent
      .order("desc")
      .take(args.count);
    return numbers.toReversed().map((number) => number.value);
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

    const exists = (await ctx.tree.get(args.value)) !== null;
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

export const numberAtIndex = queryWithNumbers({
  args: { index: v.number() },
  handler: async (ctx, args) => {
    return ctx.tree.at(args.index);
  },
});

export const countNumbers = queryWithNumbers({
  handler: async (ctx) => {
    return await ctx.tree.count();
  },
});

export const backfillBTree = mutationWithNumbers({
  args: {},
  handler: async (ctx) => {
    for await (const doc of ctx.db.query("numbers")) {
      await ctx.db.patch("numbers", doc._id, {});
    }
  },
});
