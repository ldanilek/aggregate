import { v } from "convex/values";
import { query } from "./_generated/server";
import { atIndexHandler, countHandler } from "./btree";
import { mutationWithBTree } from "./mutationWithBTree";

// Write your Convex functions in any file inside this directory (`convex`).
// See https://docs.convex.dev/functions for more.

// You can read data from the database via a query:
export const listNumbers = query({
  // Validators for arguments.
  args: {
    count: v.number(),
  },

  // Query implementation.
  handler: async (ctx, args) => {
    //// Read the database as many times as you need here.
    //// See https://docs.convex.dev/database/reading-data.
    const numbers = await ctx.db
      .query("numbers")
      // Ordered by _creationTime, return most recent
      .order("desc")
      .take(args.count);
    return numbers.toReversed().map((number) => number.value);
  },
});

const mutationWithNumbers = mutationWithBTree({
  tableName: "numbers",
  btreeName: "numbers",
  getKey: (doc) => doc.value,
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
    return await atIndexHandler(ctx, { name: "numbers", index: args.index });
  },
});

export const countNumbers = query({
  handler: async (ctx) => {
    return countHandler(ctx, { name: "numbers" });
  },
});
