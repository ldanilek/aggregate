import { v } from "convex/values";
import { query, mutation, action, DatabaseReader, DatabaseWriter } from "./_generated/server";
import { api } from "./_generated/api";
import { Doc, Id } from "./_generated/dataModel";

export async function insertHandler(ctx: { db: DatabaseWriter }, args: { name: string, key: any, value: Id<"numbers"> }) {
  const tree = await getOrCreateTree(ctx.db, args.name);
  const pushUp = await insertIntoNode(ctx.db, tree.root, args.key, args.value);
  if (pushUp) {
    const newRoot = await ctx.db.insert("btreeNode", {
      keys: [pushUp.key],
      values: [pushUp.value],
      subtrees: [pushUp.leftSubtree, pushUp.rightSubtree],
      count: pushUp.leftSubtreeCount + pushUp.rightSubtreeCount + 1,
    });
    await ctx.db.patch(tree._id, {
      root: newRoot,
    });
  }
}

export const insert = mutation({
  args: {
    name: v.string(),
    key: v.any(),
    value: v.id("numbers"),
  },
  handler: insertHandler,
});

export const count = query({
  args: { name: v.string() },
  handler: async (ctx, args) => {
    const tree = (await getTree(ctx.db, args.name))!;
    const root = (await ctx.db.get(tree.root))!;
    return root.count;
  }
});

export const atIndex = query({
  args: { name: v.string(), index: v.number() },
  handler: async (ctx, args) => {
    const tree = (await getTree(ctx.db, args.name))!;
    return await atIndexInNode(ctx.db, tree.root, args.index);
  },
});

export type KeyValue = {
  key: any;
  value: Id<"numbers">;
};

async function atIndexInNode(
  db: DatabaseReader,
  node: Id<"btreeNode">,
  index: number,
): Promise<KeyValue> {
  const n = (await db.get(node))!;
  if (index >= n.count) {
    throw new Error(`index ${index} too big for node ${n._id}`);
  }
  if (n.subtrees.length === 0) {
    return {
      key: n.keys[index],
      value: n.values[index],
    };
  }
  const subCounts = await subtreeCounts(db, n);
  for (let i = 0; i < subCounts.length; i++) {
    if (index < subCounts[i]) {
      return await atIndexInNode(db, n.subtrees[i], index);
    }
    index -= subCounts[i];
    if (index === 0) {
      return {
        key: n.keys[i],
        value: n.values[i],
      };
    }
    index--;
  }
  throw new Error(`remaing index ${index} for node ${n._id}`);
}

async function subtreeCounts(
  db: DatabaseReader,
  node: Doc<"btreeNode">,
) {
  return await Promise.all(node.subtrees.map(async (subtree) => {
    const s = (await db.get(subtree))!;
    return s.count;
  }));
}

function sum(nums: number[]) {
  return nums.reduce((acc, n) => (acc + n), 0);
}

type PushUp = {
  leftSubtree: Id<"btreeNode">,
  rightSubtree: Id<"btreeNode">,
  leftSubtreeCount: number,
  rightSubtreeCount: number,
  key: any,
  value: Id<"numbers">,
};

const MAX_NODE_SIZE = 4;
const MIN_NODE_SIZE = 2;

async function insertIntoNode(
  db: DatabaseWriter,
  node: Id<"btreeNode">,
  key: any,
  value: Id<"numbers">,
): Promise<PushUp | null> {
  const n = (await db.get(node))!;
  let i = 0;
  for (; i < n.keys.length; i++) {
    const compare = compareKeys(key, n.keys[i]);
    if (compare === -1) {
      // if key < n.keys[i], we've found the index.
      break;
    }
  }
  // insert key before index i
  if (n.subtrees.length > 0) {
    // insert into subtree
    const pushUp = await insertIntoNode(db, n.subtrees[i], key, value);
    if (pushUp) {
      await db.patch(node, {
        keys: [...n.keys.slice(0, i), pushUp.key, ...n.keys.slice(i)],
        values: [...n.values.slice(0, i), pushUp.value, ...n.values.slice(i)],
        subtrees: [...n.subtrees.slice(0, i), pushUp.leftSubtree, pushUp.rightSubtree, ...n.subtrees.slice(i+1)],
      });
    }
  } else {
    await db.patch(node, {
      keys: [...n.keys.slice(0, i), key, ...n.keys.slice(i)],
      values: [...n.values.slice(0, i), value, ...n.values.slice(i)],
    });
  }
  await db.patch(node, {
    count: n.count + 1,
  });

  const newN = (await db.get(node))!;
  if (newN.keys.length > MAX_NODE_SIZE) {
    if (newN.keys.length !== MAX_NODE_SIZE + 1
      || newN.keys.length !== 2 * MIN_NODE_SIZE + 1) {
      throw new Error(`bad ${newN.keys.length}`);
    }
    const subCounts = await subtreeCounts(db, newN);
    const leftCount = MIN_NODE_SIZE + sum(subCounts.length ? subCounts.slice(0, MIN_NODE_SIZE+1) : []);
    const rightCount = MIN_NODE_SIZE + sum(subCounts.length ? subCounts.slice(MIN_NODE_SIZE+1) : []);
    if (leftCount + rightCount + 1 !== newN.count) {
      throw new Error(`bad count split ${leftCount} ${rightCount} ${newN.count}`);
    }
    await db.patch(node, {
      keys: newN.keys.slice(0, MIN_NODE_SIZE),
      values: newN.values.slice(0, MIN_NODE_SIZE),
      subtrees: newN.subtrees.length ? newN.subtrees.slice(0, MIN_NODE_SIZE+1) : [],
      count: leftCount,
    });
    const splitN = await db.insert("btreeNode", {
      keys: newN.keys.slice(MIN_NODE_SIZE+1),
      values: newN.values.slice(MIN_NODE_SIZE+1),
      subtrees: newN.subtrees.length ? newN.subtrees.slice(MIN_NODE_SIZE+1) : [],
      count: rightCount,
    });
    return {
      key: newN.keys[MIN_NODE_SIZE],
      value: newN.values[MIN_NODE_SIZE],
      leftSubtree: node,
      rightSubtree: splitN,
      leftSubtreeCount: leftCount,
      rightSubtreeCount: rightCount,
    };
  }
  return null;
}

function compareKeys(k1: any, k2: any) {
  // TODO: compare all values
  // this mostly works for numbers.
  if (k1 < k2) {
    return -1;
  }
  if (k1 === k2) {
    return 0;
  }
  return 1;
}

async function getTree(db: DatabaseReader, name: string) {
  return await db.query("btree").withIndex("name", q=>q.eq("name", name)).unique();
}

async function getOrCreateTree(db: DatabaseWriter, name: string) {
  const originalTree = await getTree(db, name);
  if (originalTree) {
    return originalTree;
  }
  const root = await db.insert("btreeNode", {
    keys: [],
    values: [],
    subtrees: [],
    count: 0,
  });
  const id = await db.insert("btree", {
    name,
    root,
  });
  const newTree = await db.get(id);
  return newTree!;
}
