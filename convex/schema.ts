// NOTE: You can remove this file. Declaring the shape
// of the database is entirely optional in Convex.
// See https://docs.convex.dev/database/schemas.

import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  numbers: defineTable({
    value: v.number(),
  }),
  btree: defineTable({
    name: v.string(),
    root: v.id("btreeNode"),
  }).index("name", ["name"]),
  btreeNode: defineTable({
    keys: v.array(v.any()),
    values: v.array(v.any()),
    subtrees: v.array(v.id("btreeNode")),
    count: v.number(),
    sum: v.number(),
  }),
});
