
import { defineComponent } from "convex/server";
import { v } from "convex/values";

export default defineComponent("btree", {
  args: { MAX_NODE_SIZE: v.number() },
});

