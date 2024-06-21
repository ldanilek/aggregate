import { v } from "convex/values";
import {
  query,
  mutation,
  action,
  DatabaseReader,
  DatabaseWriter,
} from "./_generated/server";
import { api } from "./_generated/api";
import { Doc, Id } from "./_generated/dataModel";

export async function insertHandler(
  ctx: { db: DatabaseWriter },
  args: { name: string; key: any; value: Id<"numbers"> }
) {
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

export async function deleteHandler(
  ctx: { db: DatabaseWriter },
  args: { name: string; key: any }
) {
  const tree = (await getTree(ctx.db, args.name))!;
  await deleteFromNode(ctx.db, tree.root, args.key);
  const root = (await ctx.db.get(tree.root))!;
  if (root.keys.length === 0 && root.subtrees.length === 1) {
    console.log(
      `collapsing root ${root._id} because its only child is ${root.subtrees[0]}`
    );
    await ctx.db.patch(tree._id, {
      root: root.subtrees[0],
    });
    await ctx.db.delete(root._id);
  }
}

export const deleteKey = mutation({
  args: {
    name: v.string(),
    key: v.any(),
  },
  handler: deleteHandler,
});

export const count = query({
  args: { name: v.string() },
  handler: async (ctx, args) => {
    const tree = (await getTree(ctx.db, args.name))!;
    if (!tree) {
      return 0;
    }
    const root = (await ctx.db.get(tree.root))!;
    return root.count;
  },
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

async function deleteFromNode(
  db: DatabaseWriter,
  node: Id<"btreeNode">,
  key: any
) {
  let n = (await db.get(node))!;
  let i = 0;
  for (; i < n.keys.length; i++) {
    const compare = compareKeys(key, n.keys[i]);
    if (compare === -1) {
      // if key < n.keys[i], recurse to the left of index i
      break;
    }
    if (compare === 0) {
      console.log(`found key ${key} in node ${n._id}`);
      // we've found the key. delete it.
      if (n.subtrees.length === 0) {
        // if this is a leaf node, just delete the key
        await db.patch(node, {
          keys: [...n.keys.slice(0, i), ...n.keys.slice(i + 1)],
          values: [...n.values.slice(0, i), ...n.values.slice(i + 1)],
          count: n.count - 1,
        });
        return;
      }
      // if this is an internal node, replace the key with the predecessor
      const predecessor = await negativeIndexInNode(db, n.subtrees[i], 0);
      console.log(`replacing ${key} with predecessor ${predecessor.key}`);
      await db.patch(node, {
        keys: [...n.keys.slice(0, i), predecessor.key, ...n.keys.slice(i + 1)],
        values: [
          ...n.values.slice(0, i),
          predecessor.value,
          ...n.values.slice(i + 1),
        ],
      });
      n = (await db.get(node))!;
      // From now on, we're deleting the predecessor from the left subtree
      key = predecessor.key;
      break;
    }
  }
  // delete from subtree i
  if (n.subtrees.length === 0) {
    throw new Error(`key ${key} not found in node ${n._id}`);
  }
  await deleteFromNode(db, n.subtrees[i], key);
  await db.patch(node, {
    count: n.count - 1,
  });

  // Now we need to check if the subtree at index i is too small
  const deficientSubtree = (await db.get(n.subtrees[i]))!;
  if (deficientSubtree.keys.length < MIN_NODE_SIZE) {
    console.log(`deficient subtree ${deficientSubtree._id}`);
    // If the subtree is too small, we need to rebalance
    if (i > 0) {
      // Try to move a key from the left sibling
      const leftSibling = (await db.get(n.subtrees[i - 1]))!;
      if (leftSibling.keys.length > MIN_NODE_SIZE) {
        console.log(`rotating right with left sibling ${leftSibling._id}`);
        // Rotate right
        const grandchild = leftSibling.subtrees.length
          ? await db.get(leftSibling.subtrees[leftSibling.subtrees.length - 1])
          : null;
        const grandchildCount = grandchild ? grandchild.count : 0;
        await db.patch(deficientSubtree._id, {
          keys: [n.keys[i - 1], ...deficientSubtree.keys],
          values: [n.values[i - 1], ...deficientSubtree.values],
          subtrees: grandchild
            ? [grandchild._id, ...deficientSubtree.subtrees]
            : [],
          count: deficientSubtree.count + 1 + grandchildCount,
        });
        await db.patch(leftSibling._id, {
          keys: leftSibling.keys.slice(0, leftSibling.keys.length - 1),
          values: leftSibling.values.slice(0, leftSibling.values.length - 1),
          subtrees: grandchild
            ? leftSibling.subtrees.slice(0, leftSibling.subtrees.length - 1)
            : [],
          count: leftSibling.count - grandchildCount - 1,
        });
        await db.patch(node, {
          keys: [
            ...n.keys.slice(0, i - 1),
            leftSibling.keys[leftSibling.keys.length - 1],
            ...n.keys.slice(i),
          ],
          values: [
            ...n.values.slice(0, i - 1),
            leftSibling.values[leftSibling.values.length - 1],
            ...n.values.slice(i),
          ],
        });
        return;
      }
    }
    if (i < n.subtrees.length - 1) {
      // Try to move a key from the right sibling
      const rightSibling = (await db.get(n.subtrees[i + 1]))!;
      if (rightSibling.keys.length > MIN_NODE_SIZE) {
        console.log(`rotating left with right sibling ${rightSibling._id}`);
        // Rotate left
        const grandchild = rightSibling.subtrees.length
          ? await db.get(rightSibling.subtrees[0])
          : null;
        const grandchildCount = grandchild ? grandchild.count : 0;
        await db.patch(deficientSubtree._id, {
          keys: [...deficientSubtree.keys, n.keys[i]],
          values: [...deficientSubtree.values, n.values[i]],
          subtrees: grandchild
            ? [...deficientSubtree.subtrees, grandchild._id]
            : [],
          count: deficientSubtree.count + 1 + grandchildCount,
        });
        await db.patch(rightSibling._id, {
          keys: rightSibling.keys.slice(1),
          values: rightSibling.values.slice(1),
          subtrees: grandchild ? rightSibling.subtrees.slice(1) : [],
          count: rightSibling.count - grandchildCount - 1,
        });
        await db.patch(node, {
          keys: [
            ...n.keys.slice(0, i),
            rightSibling.keys[0],
            ...n.keys.slice(i + 1),
          ],
          values: [
            ...n.values.slice(0, i),
            rightSibling.values[0],
            ...n.values.slice(i + 1),
          ],
        });
        return;
      }
    }
    // If we can't rotate, we need to merge
    if (i > 0) {
      console.log(`merging with left sibling`);
      // Merge with left sibling
      await mergeNodes(db, n, i - 1);
    } else {
      console.log(`merging with right sibling`);
      // Merge with right sibling
      await mergeNodes(db, n, i);
    }
  }
}

async function mergeNodes(
  db: DatabaseWriter,
  parent: Doc<"btreeNode">,
  leftIndex: number
) {
  const left = (await db.get(parent.subtrees[leftIndex]))!;
  const right = (await db.get(parent.subtrees[leftIndex + 1]))!;
  console.log(`merging ${right._id} into ${left._id}`);
  await db.patch(left._id, {
    keys: [...left.keys, parent.keys[leftIndex], ...right.keys],
    values: [...left.values, parent.values[leftIndex], ...right.values],
    subtrees: [...left.subtrees, ...right.subtrees],
    count: left.count + 1 + right.count,
  });
  await db.patch(parent._id, {
    keys: [
      ...parent.keys.slice(0, leftIndex),
      ...parent.keys.slice(leftIndex + 1),
    ],
    values: [
      ...parent.values.slice(0, leftIndex),
      ...parent.values.slice(leftIndex + 1),
    ],
    subtrees: [
      ...parent.subtrees.slice(0, leftIndex + 1),
      ...parent.subtrees.slice(leftIndex + 2),
    ],
  });
  await db.delete(right._id);
}

// index 0 starts at the right
async function negativeIndexInNode(
  db: DatabaseReader,
  node: Id<"btreeNode">,
  index: number
): Promise<KeyValue> {
  const n = (await db.get(node))!;
  return await atIndexInNode(db, node, n.count - index - 1);
}

async function atIndexInNode(
  db: DatabaseReader,
  node: Id<"btreeNode">,
  index: number
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

async function subtreeCounts(db: DatabaseReader, node: Doc<"btreeNode">) {
  return await Promise.all(
    node.subtrees.map(async (subtree) => {
      const s = (await db.get(subtree))!;
      return s.count;
    })
  );
}

function sum(nums: number[]) {
  return nums.reduce((acc, n) => acc + n, 0);
}

type PushUp = {
  leftSubtree: Id<"btreeNode">;
  rightSubtree: Id<"btreeNode">;
  leftSubtreeCount: number;
  rightSubtreeCount: number;
  key: any;
  value: Id<"numbers">;
};

const MAX_NODE_SIZE = 4;
const MIN_NODE_SIZE = 2;

async function insertIntoNode(
  db: DatabaseWriter,
  node: Id<"btreeNode">,
  key: any,
  value: Id<"numbers">
): Promise<PushUp | null> {
  const n = (await db.get(node))!;
  let i = 0;
  for (; i < n.keys.length; i++) {
    const compare = compareKeys(key, n.keys[i]);
    if (compare === -1) {
      // if key < n.keys[i], we've found the index.
      break;
    }
    if (compare === 0) {
      throw new Error(`key ${key} already exists in node ${n._id}`);
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
        subtrees: [
          ...n.subtrees.slice(0, i),
          pushUp.leftSubtree,
          pushUp.rightSubtree,
          ...n.subtrees.slice(i + 1),
        ],
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
    if (
      newN.keys.length !== MAX_NODE_SIZE + 1 ||
      newN.keys.length !== 2 * MIN_NODE_SIZE + 1
    ) {
      throw new Error(`bad ${newN.keys.length}`);
    }
    console.log(`splitting node ${newN._id} at ${newN.keys[MIN_NODE_SIZE]}`);
    const subCounts = await subtreeCounts(db, newN);
    const leftCount =
      MIN_NODE_SIZE +
      sum(subCounts.length ? subCounts.slice(0, MIN_NODE_SIZE + 1) : []);
    const rightCount =
      MIN_NODE_SIZE +
      sum(subCounts.length ? subCounts.slice(MIN_NODE_SIZE + 1) : []);
    if (leftCount + rightCount + 1 !== newN.count) {
      throw new Error(
        `bad count split ${leftCount} ${rightCount} ${newN.count}`
      );
    }
    await db.patch(node, {
      keys: newN.keys.slice(0, MIN_NODE_SIZE),
      values: newN.values.slice(0, MIN_NODE_SIZE),
      subtrees: newN.subtrees.length
        ? newN.subtrees.slice(0, MIN_NODE_SIZE + 1)
        : [],
      count: leftCount,
    });
    const splitN = await db.insert("btreeNode", {
      keys: newN.keys.slice(MIN_NODE_SIZE + 1),
      values: newN.values.slice(MIN_NODE_SIZE + 1),
      subtrees: newN.subtrees.length
        ? newN.subtrees.slice(MIN_NODE_SIZE + 1)
        : [],
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
  return await db
    .query("btree")
    .withIndex("name", (q) => q.eq("name", name))
    .unique();
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
