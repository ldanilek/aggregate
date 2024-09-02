import { v } from "convex/values";
import { Id } from "./_generated/dataModel";
import { DatabaseReader, query } from "./_generated/server";
import { getTree, p } from "./btree";

export const display = query({
  args: { },
  handler: async (ctx) => {
    const tree = await getTree(ctx.db);
    if (!tree) {
      return "empty";
    }
    return await displayNode(ctx.db, tree.root);
  },
});

async function displayNode(
  db: DatabaseReader,
  node: Id<"btreeNode">,
  depth: number = 0
) {
  const n = (await db.get(node))!;
  for (let i = 0; i < n.items.length; i++) {
    if (n.subtrees.length > 0) {
      await displayNode(db, n.subtrees[i], depth + 1);
    }
    console.log(" ".repeat(depth) + p(n.items[i].k));
  }
  if (n.subtrees.length > 0) {
    await displayNode(db, n.subtrees[n.subtrees.length - 1], depth + 1);
  }
}

export const dump = query({
  args: { },
  returns: v.string(),
  handler: async (ctx) => {
    return await dumpTree(ctx.db);
  },
});

export async function dumpTree(db: DatabaseReader) {
  const t = (await getTree(db))!;
  return dumpNode(db, t.root);
}

/// Prints keys in-order, with brackets for each node.
async function dumpNode(
  db: DatabaseReader,
  node: Id<"btreeNode">
): Promise<string> {
  const n = (await db.get(node))!;
  let s = "[";
  if (n.subtrees.length === 0) {
    s += n.items.map((i) => i.k).map(p).join(", ");
  } else {
    const subtrees = await Promise.all(
      n.subtrees.map((subtree) => dumpNode(db, subtree))
    );
    for (let i = 0; i < n.items.length; i++) {
      s += `${subtrees[i]}, ${p(n.items[i].k)}, `;
    }
    s += subtrees[n.items.length];
  }
  s += "]";
  return s;
}

export const inspectNode = query({
  args: { node: v.optional(v.string()) },
  returns: v.null(),
  handler: async (ctx, args) => {
    const tree = await getTree(ctx.db);
    if (!tree) {
      console.log("no tree");
      return;
    }
    let n = await ctx.db.get(tree.root);
    if (args.node) {
      n = await ctx.db.get(args.node as Id<"btreeNode">);
    }
    if (!n) {
      console.log("no node");
      return;
    }
    console.log('btreeNode', n._id);
    console.log('aggregate', n.aggregate);
    for (let i = 0; i < n.items.length; i++) {
      if (n.subtrees.length > 0) {
        console.log('subtree', n.subtrees[i]);
      }
      console.log('item', n.items[i]);
    }
    if (n.subtrees.length > 0) {
      console.log('subtree', n.subtrees[n.subtrees.length - 1]);
    }
  },
});