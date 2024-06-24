import { Value as ConvexValue, v } from "convex/values";
import {
  DatabaseReader,
  DatabaseWriter,
  internalQuery,
  internalMutation,
} from "../_generated/server";
import { Doc, Id } from "../_generated/dataModel";
import { compareValues } from "./compare";

const BTREE_DEBUG = process.env.BTREE_DEBUG === "true";

export type Key = ConvexValue;
export type Value = ConvexValue;

function p(v: ConvexValue): string {
  return v?.toString() ?? "undefined";
}

function log(s: string) {
  if (BTREE_DEBUG) {
    log(s);
  }
}

export async function insertHandler(
  ctx: { db: DatabaseWriter },
  args: { name: string; key: Key; value: Value }
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

export const insert = internalMutation({
  args: {
    name: v.string(),
    key: v.any(),
    value: v.any(),
  },
  handler: insertHandler,
});

export async function deleteHandler(
  ctx: { db: DatabaseWriter },
  args: { name: string; key: Key }
) {
  const tree = await getOrCreateTree(ctx.db, args.name);
  await deleteFromNode(ctx.db, tree.root, args.key);
  const root = (await ctx.db.get(tree.root))!;
  if (root.keys.length === 0 && root.subtrees.length === 1) {
    log(
      `collapsing root ${root._id} because its only child is ${root.subtrees[0]}`
    );
    await ctx.db.patch(tree._id, {
      root: root.subtrees[0],
    });
    await ctx.db.delete(root._id);
  }
}

export const deleteKey = internalMutation({
  args: {
    name: v.string(),
    key: v.any(),
  },
  handler: deleteHandler,
});

export async function validateTree(
  ctx: { db: DatabaseReader },
  args: { name: string }
) {
  const tree = await getTree(ctx.db, args.name);
  if (!tree) {
    return;
  }
  await validateNode(ctx.db, tree.root, 0);
}

type ValidationResult = {
  min: Key;
  max: Key;
  height: number;
};

async function validateNode(
  db: DatabaseReader,
  node: Id<"btreeNode">,
  depth: number
): Promise<ValidationResult> {
  const n = await db.get(node);
  if (!n) {
    throw new Error(`missing node ${node}`);
  }
  if (n.keys.length > MAX_NODE_SIZE) {
    throw new Error(`node ${node} exceeds max size`);
  }
  if (depth > 0 && n.keys.length < MIN_NODE_SIZE) {
    throw new Error(`non-root node ${node} has less than min-size`);
  }
  if (n.keys.length !== n.values.length) {
    throw new Error(`node ${node} keys do not match values`);
  }
  if (n.subtrees.length > 0 && n.keys.length + 1 !== n.subtrees.length) {
    throw new Error(`node ${node} keys do not match subtrees`);
  }
  if (n.subtrees.length > 0 && n.keys.length === 0) {
    throw new Error(`node ${node} one subtree but no keys`);
  }
  // Keys are in increasing order
  for (let i = 1; i < n.keys.length; i++) {
    if (compareKeys(n.keys[i - 1], n.keys[i]) !== -1) {
      throw new Error(`node ${node} keys not in order`);
    }
  }
  const validatedSubtrees = await Promise.all(
    n.subtrees.map((subtree) => validateNode(db, subtree, depth + 1))
  );
  for (let i = 0; i < n.subtrees.length; i++) {
    // Each subtree's min is greater than the key at the prior index
    if (i > 0 && compareKeys(validatedSubtrees[i].min, n.keys[i - 1]) !== 1) {
      throw new Error(`subtree ${i} min is too small for node ${node}`);
    }
    // Each subtree's max is less than the key at the same index
    if (
      i < n.keys.length &&
      compareKeys(validatedSubtrees[i].max, n.keys[i]) !== -1
    ) {
      throw new Error(`subtree ${i} max is too large for node ${node}`);
    }
  }
  // All subtrees have the same height.
  const heights = validatedSubtrees.map((s) => s.height);
  for (let i = 1; i < heights.length; i++) {
    if (heights[i] !== heights[0]) {
      throw new Error(`subtree ${i} has different height from others`);
    }
  }

  // Node count matches sum of subtree counts plus key count.
  const counts = await subtreeCounts(db, n);
  if (sum(counts) + n.keys.length !== n.count) {
    throw new Error(`node ${node} count does not match subtrees`);
  }

  const max =
    validatedSubtrees.length > 0
      ? validatedSubtrees[validatedSubtrees.length - 1].max
      : n.keys[n.keys.length - 1];
  const min =
    validatedSubtrees.length > 0 ? validatedSubtrees[0].min : n.keys[0];
  const height = validatedSubtrees.length > 0 ? 1 + heights[0] : 0;
  return { min, max, height };
}

export async function dumpTree(db: DatabaseReader, name: string) {
  const t = (await getTree(db, name))!;
  return dumpNode(db, t.root);
}

async function dumpNode(
  db: DatabaseReader,
  node: Id<"btreeNode">
): Promise<string> {
  const n = (await db.get(node))!;
  let s = "[";
  if (n.subtrees.length === 0) {
    s += n.keys.map(p).join(", ");
  } else {
    const subtrees = await Promise.all(
      n.subtrees.map((subtree) => dumpNode(db, subtree))
    );
    for (let i = 0; i < n.keys.length; i++) {
      s += `${subtrees[i]}, ${p(n.keys[i])}, `;
    }
    s += subtrees[n.keys.length];
  }
  s += "]";
  return s;
}

export const count = internalQuery({
  args: { name: v.string() },
  handler: countHandler,
});

export async function countHandler(
  ctx: { db: DatabaseReader },
  args: { name: string }
) {
  const tree = (await getTree(ctx.db, args.name))!;
  if (!tree) {
    return 0;
  }
  const root = (await ctx.db.get(tree.root))!;
  return root.count;
}

/// Count of keys that are *strictly* between k1 and k2.
/// If k1 or k2 are undefined, that bound is unlimited.
export async function countBetweenHandler(
  ctx: { db: DatabaseReader },
  args: { name: string; k1?: Key; k2?: Key }
) {
  const tree = (await getTree(ctx.db, args.name))!;
  return await countBetweenInNode(ctx.db, tree.root, args.k1, args.k2);
}

export const countBetween = internalQuery({
  args: { name: v.string(), k1: v.optional(v.any()), k2: v.optional(v.any()) },
  handler: countBetweenHandler,
});

async function countBetweenInNode(
  db: DatabaseReader,
  node: Id<"btreeNode">,
  k1?: Key,
  k2?: Key
) {
  const n = (await db.get(node))!;
  const subCounts = await subtreeCounts(db, n);
  let count = 0;
  let i = 0;
  let foundLeftSide = false;
  let foundLeftSidePreviously = false;
  for (; i < n.keys.length; i++) {
    foundLeftSidePreviously = foundLeftSide;
    const containsK1 = k1 === undefined || compareKeys(k1, n.keys[i]) === -1;
    const containsK2 = k2 !== undefined && compareKeys(k2, n.keys[i]) !== 1;
    if (!foundLeftSide) {
      if (containsK1) {
        // k1 is within n.subtree[i].
        foundLeftSide = true;
        if (n.subtrees.length > 0) {
          count += await countBetweenInNode(
            db,
            n.subtrees[i],
            k1,
            containsK2 ? k2 : undefined
          );
        }
      }
    }
    if (foundLeftSide) {
      if (containsK2) {
        // k2 is within n.subtree[i].
        // So i is the final index to look at.
        break;
      }
      // count n.keys[i]
      count++;
      // count n.subtrees[i] if we didn't already
      if (n.subtrees.length > 0 && foundLeftSidePreviously) {
        count += subCounts[i];
      }
    }
  }
  if (n.subtrees.length > 0) {
    count += await countBetweenInNode(
      db,
      n.subtrees[i],
      foundLeftSide ? undefined : k1,
      k2
    );
  }
  return count;
}

export async function getHandler(
  ctx: { db: DatabaseReader },
  args: { name: string; key: Key }
) {
  const tree = (await getTree(ctx.db, args.name))!;
  return await getInNode(ctx.db, tree.root, args.key);
}

export const get = internalQuery({
  args: { name: v.string(), key: v.any() },
  handler: getHandler,
});

async function getInNode(
  db: DatabaseReader,
  node: Id<"btreeNode">,
  key: Key
): Promise<KeyValue | null> {
  const n = (await db.get(node))!;
  let i = 0;
  for (; i < n.keys.length; i++) {
    const compare = compareKeys(key, n.keys[i]);
    if (compare === -1) {
      // if key < n.keys[i], recurse to the left of index i
      break;
    }
    if (compare === 0) {
      return {
        key: n.keys[i],
        value: n.values[i],
      };
    }
  }
  if (n.subtrees.length === 0) {
    return null;
  }
  return await getInNode(db, n.subtrees[i], key);
}

export const atIndex = internalQuery({
  args: { name: v.string(), index: v.number() },
  handler: atIndexHandler,
});

export async function atIndexHandler(
  ctx: { db: DatabaseReader },
  args: { name: string; index: number }
) {
  const tree = (await getTree(ctx.db, args.name))!;
  return await atIndexInNode(ctx.db, tree.root, args.index);
}

export async function rankHandler(
  ctx: { db: DatabaseReader },
  args: { name: string; key: Key }
) {
  const tree = (await getTree(ctx.db, args.name))!;
  return await rankInNode(ctx.db, tree.root, args.key);
}

export const rank = internalQuery({
  args: { name: v.string(), key: v.any() },
  handler: rankHandler,
});

async function rankInNode(
  db: DatabaseReader,
  node: Id<"btreeNode">,
  key: Key
): Promise<number | null> {
  const n = (await db.get(node))!;
  let i = 0;
  for (; i < n.keys.length; i++) {
    const compare = compareKeys(key, n.keys[i]);
    if (compare === -1) {
      // if key < n.keys[i], recurse to the left of index i
      break;
    }
    if (compare === 0) {
      if (n.subtrees.length === 0) {
        return i;
      }
      const subCounts = await subtreeCounts(db, n);
      return sum(subCounts.slice(0, i)) + i;
    }
  }
  if (n.subtrees.length === 0) {
    return null;
  }
  const subCounts = await subtreeCounts(db, n);
  const rankInSubtree = await rankInNode(db, n.subtrees[i], key);
  if (rankInSubtree === null) {
    return null;
  }
  return sum(subCounts.slice(0, i)) + i + rankInSubtree;
}

export type KeyValue = {
  key: Key;
  value: Value;
};

async function deleteFromNode(
  db: DatabaseWriter,
  node: Id<"btreeNode">,
  key: Key
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
      log(`found key ${p(key)} in node ${n._id}`);
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
      log(`replacing ${p(key)} with predecessor ${p(predecessor.key)}`);
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
    log(`key ${p(key)} not found in node ${n._id}`);
    // TODO: consider throwing.
    // For now we don't throw to support patching to backfill.
    return;
  }
  await deleteFromNode(db, n.subtrees[i], key);
  await db.patch(node, {
    count: n.count - 1,
  });

  // Now we need to check if the subtree at index i is too small
  const deficientSubtree = (await db.get(n.subtrees[i]))!;
  if (deficientSubtree.keys.length < MIN_NODE_SIZE) {
    log(`deficient subtree ${deficientSubtree._id}`);
    // If the subtree is too small, we need to rebalance
    if (i > 0) {
      // Try to move a key from the left sibling
      const leftSibling = (await db.get(n.subtrees[i - 1]))!;
      if (leftSibling.keys.length > MIN_NODE_SIZE) {
        log(`rotating right with left sibling ${leftSibling._id}`);
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
        log(`rotating left with right sibling ${rightSibling._id}`);
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
      log(`merging with left sibling`);
      // Merge with left sibling
      await mergeNodes(db, n, i - 1);
    } else {
      log(`merging with right sibling`);
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
  log(`merging ${right._id} into ${left._id}`);
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
  key: Key;
  value: Value;
};

const MAX_NODE_SIZE = 4;
const MIN_NODE_SIZE = 2;

async function insertIntoNode(
  db: DatabaseWriter,
  node: Id<"btreeNode">,
  key: Key,
  value: Value
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
      throw new Error(`key ${p(key)} already exists in node ${n._id}`);
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
    log(`splitting node ${newN._id} at ${newN.keys[MIN_NODE_SIZE]}`);
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

function compareKeys(k1: Key, k2: Key) {
  return compareValues(k1, k2);
}

export async function getTree(db: DatabaseReader, name: string) {
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
