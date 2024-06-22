import { v } from "convex/values";
import { query, mutation } from "./_generated/server";
import { atIndexHandler, deleteHandler, insertHandler } from "./btree";

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

// You can write data to the database via a mutation:
export const addNumber = mutation({
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

    await insertHandler(ctx, {
      name: "numbers",
      key: args.value,
      value: id,
    });

    console.log("Added new document with id:", id);
    // Optionally, return a value from your mutation.
    // return id;
  },
});

export const removeNumber = mutation({
  args: { number: v.id("numbers") },
  handler: async (ctx, args) => {
    const n = (await ctx.db.get(args.number))!;
    await ctx.db.delete(args.number);
    await deleteHandler(ctx, {
      name: "numbers",
      key: n.value,
    });
  },
});

export const numberAtIndex = query({
  args: { index: v.number() },
  handler: async (ctx, args) => {
    return await atIndexHandler(ctx, { name: "numbers", index: args.index });
  },
});