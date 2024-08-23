import { Value as ConvexValue, v } from "convex/values";
import {
  componentArg,
  DatabaseReader,
  DatabaseWriter,
  mutation,
  query,
} from "./_generated/server";
import { Doc, Id } from "./_generated/dataModel";
import { compareValues } from "./compare";
import { Aggregate, Item } from "./schema";
import { FunctionHandle, GenericDocument } from "convex/server";
import { triggerArgsValidator } from "../triggers/types";

const BTREE_DEBUG = false;

export type Key = ConvexValue;
export type Value = ConvexValue;

function p(v: ConvexValue): string {
  return v?.toString() ?? "undefined";
}

function log(s: string) {
  if (BTREE_DEBUG) {
    console.log(s);
  }
}

export async function insertHandler(
  ctx: { db: DatabaseWriter },
  args: { key: Key; value: Value, summand?: number }
) {
  const tree = await mustGetTree(ctx.db);
  const summand = args.summand ?? 0;
  const pushUp = await insertIntoNode(
    ctx,
    tree.root,
    { k: args.key, v: args.value, s: summand },
  );
  if (pushUp) {
    const total = add(add(pushUp.leftSubtreeCount, pushUp.rightSubtreeCount), itemAggregate(pushUp.item));
    const newRoot = await ctx.db.insert("btreeNode", {
      items: [pushUp.item],
      subtrees: [pushUp.leftSubtree, pushUp.rightSubtree],
      aggregate: total,
    });
    await ctx.db.patch(tree._id, {
      root: newRoot,
    });
  }
}

export async function deleteHandler(
  ctx: { db: DatabaseWriter },
  args: { key: Key }
) {
  const tree = await mustGetTree(ctx.db);
  await deleteFromNode(ctx, tree.root, args.key);
  const root = (await ctx.db.get(tree.root))!;
  if (root.items.length === 0 && root.subtrees.length === 1) {
    log(
      `collapsing root ${root._id} because its only child is ${root.subtrees[0]}`
    );
    await ctx.db.patch(tree._id, {
      root: root.subtrees[0],
    });
    await ctx.db.delete(root._id);
  }
}

export const validate = query({
  args: { },
  handler: validateTree,
});

export async function validateTree(
  ctx: { db: DatabaseReader },
) {
  const tree = await getTree(ctx.db);
  if (!tree) {
    return;
  }
  await validateNode(ctx, tree.root, 0);
}

type ValidationResult = {
  min?: Key;
  max?: Key;
  height: number;
};

function MAX_NODE_SIZE(ctx: any) {
  return componentArg(ctx, "MAX_NODE_SIZE");
}

function MIN_NODE_SIZE(ctx: any) {
  const max = componentArg(ctx, "MAX_NODE_SIZE");
  if (max % 2 !== 0 || max < 4) {
    throw new Error("MAX_NODE_SIZE must be even and at least 4");
  }
  return max / 2;
}

async function validateNode(
  ctx: {db: DatabaseReader},
  node: Id<"btreeNode">,
  depth: number
): Promise<ValidationResult> {
  const n = await ctx.db.get(node);
  if (!n) {
    throw new Error(`missing node ${node}`);
  }
  if (n.items.length > MAX_NODE_SIZE(ctx)) {
    throw new Error(`node ${node} exceeds max size`);
  }
  if (depth > 0 && n.items.length < MIN_NODE_SIZE(ctx)) {
    throw new Error(`non-root node ${node} has less than min-size`);
  }
  if (n.subtrees.length > 0 && n.items.length + 1 !== n.subtrees.length) {
    throw new Error(`node ${node} keys do not match subtrees`);
  }
  if (n.subtrees.length > 0 && n.items.length === 0) {
    throw new Error(`node ${node} one subtree but no keys`);
  }
  // Keys are in increasing order
  for (let i = 1; i < n.items.length; i++) {
    if (compareKeys(n.items[i - 1].k, n.items[i].k) !== -1) {
      throw new Error(`node ${node} keys not in order`);
    }
  }
  const validatedSubtrees = await Promise.all(
    n.subtrees.map((subtree) => validateNode(ctx, subtree, depth + 1))
  );
  for (let i = 0; i < n.subtrees.length; i++) {
    // Each subtree's min is greater than the key at the prior index
    if (i > 0 && compareKeys(validatedSubtrees[i].min!, n.items[i - 1].k) !== 1) {
      throw new Error(`subtree ${i} min is too small for node ${node}`);
    }
    // Each subtree's max is less than the key at the same index
    if (
      i < n.items.length &&
      compareKeys(validatedSubtrees[i].max!, n.items[i].k) !== -1
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
  const counts = await subtreeCounts(ctx.db, n);
  const atNode = nodeCounts(n);
  const acc = add(accumulate(counts), accumulate(atNode));
  if (acc.count !== n.aggregate.count) {
    throw new Error(`node ${node} count does not match subtrees`);
  }

  // Node sum matches sum of subtree sums plus key sum.
  if (acc.sum !== n.aggregate.sum) {
    throw new Error(`node ${node} sum does not match subtrees`);
  }

  const max =
    validatedSubtrees.length > 0
      ? validatedSubtrees[validatedSubtrees.length - 1].max
      : n.items[n.items.length - 1]?.k;
  const min =
    validatedSubtrees.length > 0 ? validatedSubtrees[0].min : n.items[0]?.k;
  const height = validatedSubtrees.length > 0 ? 1 + heights[0] : 0;
  return { min, max, height };
}

export async function dumpTree(db: DatabaseReader) {
  const t = (await getTree(db))!;
  return dumpNode(db, t.root);
}

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

export const count = query({
  args: { },
  handler: countHandler,
});

export async function countHandler(
  ctx: { db: DatabaseReader },
) {
  const tree = await getTree(ctx.db);
  if (!tree) {
    return 0;
  }
  const root = (await ctx.db.get(tree.root))!;
  return root.aggregate.count;
}

export const sum = query({
  args: { },
  handler: sumHandler,
});

export async function sumHandler(
  ctx: { db: DatabaseReader },
) {
  const tree = await getTree(ctx.db);
  if (!tree) {
    return 0;
  }
  const root = (await ctx.db.get(tree.root))!;
  return root.aggregate.sum;
}

/// Count of keys that are *strictly* between k1 and k2.
/// If k1 or k2 are undefined, that bound is unlimited.
export async function countBetweenHandler(
  ctx: { db: DatabaseReader },
  args: { k1?: Key; k2?: Key }
) {
  const tree = (await getTree(ctx.db))!;
  return await countBetweenInNode(ctx.db, tree.root, args.k1, args.k2);
}

export const countBetween = query({
  args: { k1: v.optional(v.any()), k2: v.optional(v.any()) },
  handler: countBetweenHandler,
});

async function countBetweenInNode(
  db: DatabaseReader,
  node: Id<"btreeNode">,
  k1?: Key,
  k2?: Key
): Promise<Aggregate> {
  const n = (await db.get(node))!;
  const subCounts = await subtreeCounts(db, n);
  let count = { count: 0, sum: 0 };
  let i = 0;
  let foundLeftSide = false;
  let foundLeftSidePreviously = false;
  for (; i < n.items.length; i++) {
    foundLeftSidePreviously = foundLeftSide;
    const containsK1 = k1 === undefined || compareKeys(k1, n.items[i].k) === -1;
    const containsK2 = k2 !== undefined && compareKeys(k2, n.items[i].k) !== 1;
    if (!foundLeftSide) {
      if (containsK1) {
        // k1 is within n.subtree[i].
        foundLeftSide = true;
        if (n.subtrees.length > 0) {
          count = add(count, await countBetweenInNode(
            db,
            n.subtrees[i],
            k1,
            containsK2 ? k2 : undefined
          ));
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
      count = add(count, itemAggregate(n.items[i]));
      // count n.subtrees[i] if we didn't already
      if (n.subtrees.length > 0 && foundLeftSidePreviously) {
        count = add(count, subCounts[i]);
      }
    }
  }
  if (n.subtrees.length > 0) {
    count = add(count, await countBetweenInNode(
      db,
      n.subtrees[i],
      foundLeftSide ? undefined : k1,
      k2
    ));
  }
  return count;
}

export async function getHandler(
  ctx: { db: DatabaseReader },
  args: { key: Key }
) {
  const tree = (await getTree(ctx.db))!;
  return await getInNode(ctx.db, tree.root, args.key);
}

export const get = query({
  args: { key: v.any() },
  handler: getHandler,
});

async function getInNode(
  db: DatabaseReader,
  node: Id<"btreeNode">,
  key: Key
): Promise<Item | null> {
  const n = (await db.get(node))!;
  let i = 0;
  for (; i < n.items.length; i++) {
    const compare = compareKeys(key, n.items[i].k);
    if (compare === -1) {
      // if key < n.keys[i], recurse to the left of index i
      break;
    }
    if (compare === 0) {
      return n.items[i];
    }
  }
  if (n.subtrees.length === 0) {
    return null;
  }
  return await getInNode(db, n.subtrees[i], key);
}

export const atIndex = query({
  args: { index: v.number() },
  handler: atIndexHandler,
});

export async function atIndexHandler(
  ctx: { db: DatabaseReader },
  args: { index: number }
) {
  const tree = (await getTree(ctx.db))!;
  return await atIndexInNode(ctx.db, tree.root, args.index);
}

export async function rankHandler(
  ctx: { db: DatabaseReader },
  args: { key: Key }
) {
  const tree = (await getTree(ctx.db))!;
  return await rankInNode(ctx.db, tree.root, args.key);
}

export const rank = query({
  args: { key: v.any() },
  handler: rankHandler,
});

async function rankInNode(
  db: DatabaseReader,
  node: Id<"btreeNode">,
  key: Key
): Promise<number | null> {
  const n = (await db.get(node))!;
  let i = 0;
  for (; i < n.items.length; i++) {
    const compare = compareKeys(key, n.items[i].k);
    if (compare === -1) {
      // if key < n.keys[i], recurse to the left of index i
      break;
    }
    if (compare === 0) {
      if (n.subtrees.length === 0) {
        return i;
      }
      const subCounts = await subtreeCounts(db, n);
      return accumulate(subCounts.slice(0, i)).count + i;
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
  return accumulate(subCounts.slice(0, i)).count + i + rankInSubtree;
}

async function deleteFromNode(
  ctx: {db: DatabaseWriter},
  node: Id<"btreeNode">,
  key: Key
): Promise<Item | null> {
  let n = (await ctx.db.get(node))!;
  let foundItem: null | Item = null;
  let i = 0;
  for (; i < n.items.length; i++) {
    const compare = compareKeys(key, n.items[i].k);
    if (compare === -1) {
      // if key < n.keys[i], recurse to the left of index i
      break;
    }
    if (compare === 0) {
      log(`found key ${p(key)} in node ${n._id}`);
      // we've found the key. delete it.
      if (n.subtrees.length === 0) {
        // if this is a leaf node, just delete the key
        await ctx.db.patch(node, {
          items: [...n.items.slice(0, i), ...n.items.slice(i + 1)],
          aggregate: sub(n.aggregate, itemAggregate(n.items[i])),
        });
        return n.items[i];
      }
      // if this is an internal node, replace the key with the predecessor
      const predecessor = await negativeIndexInNode(ctx.db, n.subtrees[i], 0);
      log(`replacing ${p(key)} with predecessor ${p(predecessor.k)}`);
      foundItem = n.items[i];
      await ctx.db.patch(node, {
        items: [...n.items.slice(0, i), predecessor, ...n.items.slice(i + 1)],
      });
      n = (await ctx.db.get(node))!;
      // From now on, we're deleting the predecessor from the left subtree
      key = predecessor.k;
      break;
    }
  }
  // delete from subtree i
  if (n.subtrees.length === 0) {
    log(`key ${p(key)} not found in node ${n._id}`);
    // TODO: consider throwing.
    // For now we don't throw to support patching to backfill.
    return null;
  }
  const deleted = await deleteFromNode(ctx, n.subtrees[i], key);
  if (!deleted) {
    return null;
  }
  if (!foundItem) {
    foundItem = deleted;
  }
  await ctx.db.patch(node, {
    aggregate: sub(n.aggregate, itemAggregate(foundItem)),
  });

  // Now we need to check if the subtree at index i is too small
  const deficientSubtree = (await ctx.db.get(n.subtrees[i]))!;
  if (deficientSubtree.items.length < MIN_NODE_SIZE(ctx)) {
    log(`deficient subtree ${deficientSubtree._id}`);
    // If the subtree is too small, we need to rebalance
    if (i > 0) {
      // Try to move a key from the left sibling
      const leftSibling = (await ctx.db.get(n.subtrees[i - 1]))!;
      if (leftSibling.items.length > MIN_NODE_SIZE(ctx)) {
        log(`rotating right with left sibling ${leftSibling._id}`);
        // Rotate right
        const grandchild = leftSibling.subtrees.length
          ? await ctx.db.get(leftSibling.subtrees[leftSibling.subtrees.length - 1])
          : null;
        const grandchildCount = grandchild ? grandchild.aggregate : { count: 0, sum: 0 };
        await ctx.db.patch(deficientSubtree._id, {
          items: [n.items[i - 1], ...deficientSubtree.items],
          subtrees: grandchild
            ? [grandchild._id, ...deficientSubtree.subtrees]
            : [],
          aggregate: add(
            add(deficientSubtree.aggregate, grandchildCount),
            itemAggregate(n.items[i - 1]),
          ),
        });
        await ctx.db.patch(leftSibling._id, {
          items: leftSibling.items.slice(0, leftSibling.items.length - 1),
          subtrees: grandchild
            ? leftSibling.subtrees.slice(0, leftSibling.subtrees.length - 1)
            : [],
          aggregate: sub(
            sub(leftSibling.aggregate, grandchildCount),
            itemAggregate(leftSibling.items[leftSibling.items.length - 1])
          ),
        });
        await ctx.db.patch(node, {
          items: [...n.items.slice(0, i - 1), leftSibling.items[leftSibling.items.length - 1], ...n.items.slice(i)],
        });
        return foundItem;
      }
    }
    if (i < n.subtrees.length - 1) {
      // Try to move a key from the right sibling
      const rightSibling = (await ctx.db.get(n.subtrees[i + 1]))!;
      if (rightSibling.items.length > MIN_NODE_SIZE(ctx)) {
        log(`rotating left with right sibling ${rightSibling._id}`);
        // Rotate left
        const grandchild = rightSibling.subtrees.length
          ? await ctx.db.get(rightSibling.subtrees[0])
          : null;
        const grandchildCount = grandchild ? grandchild.aggregate : { count: 0, sum: 0 };
        await ctx.db.patch(deficientSubtree._id, {
          items: [...deficientSubtree.items, n.items[i]],
          subtrees: grandchild
            ? [...deficientSubtree.subtrees, grandchild._id]
            : [],
          aggregate: add(add(deficientSubtree.aggregate, grandchildCount), itemAggregate(n.items[i])),
        });
        await ctx.db.patch(rightSibling._id, {
          items: rightSibling.items.slice(1),
          subtrees: grandchild ? rightSibling.subtrees.slice(1) : [],
          aggregate: sub(
            sub(rightSibling.aggregate, grandchildCount),
            itemAggregate(rightSibling.items[0]),
          ),
        });
        await ctx.db.patch(node, {
          items: [...n.items.slice(0, i), rightSibling.items[0], ...n.items.slice(i + 1)],
        });
        return foundItem;
      }
    }
    // If we can't rotate, we need to merge
    if (i > 0) {
      log(`merging with left sibling`);
      // Merge with left sibling
      await mergeNodes(ctx.db, n, i - 1);
    } else {
      log(`merging with right sibling`);
      // Merge with right sibling
      await mergeNodes(ctx.db, n, i);
    }
  }
  return foundItem;
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
    items: [...left.items, parent.items[leftIndex], ...right.items],
    subtrees: [...left.subtrees, ...right.subtrees],
    aggregate: add(add(left.aggregate, right.aggregate), itemAggregate(parent.items[leftIndex])),
  });
  await db.patch(parent._id, {
    items: [
      ...parent.items.slice(0, leftIndex),
      ...parent.items.slice(leftIndex + 1),
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
): Promise<Item> {
  const n = (await db.get(node))!;
  return await atIndexInNode(db, node, n.aggregate.count - index - 1);
}

async function atIndexInNode(
  db: DatabaseReader,
  node: Id<"btreeNode">,
  index: number
): Promise<Item> {
  const n = (await db.get(node))!;
  if (index >= n.aggregate.count) {
    throw new Error(`index ${index} too big for node ${n._id}`);
  }
  if (n.subtrees.length === 0) {
    return n.items[index];
  }
  const subCounts = await subtreeCounts(db, n);
  for (let i = 0; i < subCounts.length; i++) {
    if (index < subCounts[i].count) {
      return await atIndexInNode(db, n.subtrees[i], index);
    }
    index -= subCounts[i].count;
    if (index === 0) {
      return n.items[i];
    }
    index--;
  }
  throw new Error(`remaing index ${index} for node ${n._id}`);
}

function itemAggregate(item: Item): Aggregate {
  return { count: 1, sum: item.s };
}

function nodeCounts(node: Doc<"btreeNode">): Aggregate[] {
  return node.items.map(itemAggregate);
}

async function subtreeCounts(db: DatabaseReader, node: Doc<"btreeNode">) {
  return await Promise.all(
    node.subtrees.map(async (subtree) => {
      const s = (await db.get(subtree))!;
      return s.aggregate;
    })
  );
}

function add(a: Aggregate, b: Aggregate) {
  return {
    count: a.count + b.count,
    sum: a.sum + b.sum,
  };
}

function sub(a: Aggregate, b: Aggregate) {
  return {
    count: a.count - b.count,
    sum: a.sum - b.sum,
  };
}

function accumulate(nums: Aggregate[]) {
  return nums.reduce(add, { count: 0, sum: 0 });
}

type PushUp = {
  leftSubtree: Id<"btreeNode">;
  rightSubtree: Id<"btreeNode">;
  leftSubtreeCount: Aggregate;
  rightSubtreeCount: Aggregate;
  item: Item;
};

async function insertIntoNode(
  ctx: {db: DatabaseWriter},
  node: Id<"btreeNode">,
  item: Item,
): Promise<PushUp | null> {
  const n = (await ctx.db.get(node))!;
  let i = 0;
  for (; i < n.items.length; i++) {
    const compare = compareKeys(item.k, n.items[i].k);
    if (compare === -1) {
      // if key < n.keys[i], we've found the index.
      break;
    }
    if (compare === 0) {
      throw new Error(`key ${p(item.k)} already exists in node ${n._id}`);
    }
  }
  // insert key before index i
  if (n.subtrees.length > 0) {
    // insert into subtree
    const pushUp = await insertIntoNode(ctx, n.subtrees[i], item);
    if (pushUp) {
      await ctx.db.patch(node, {
        items: [...n.items.slice(0, i), pushUp.item, ...n.items.slice(i)],
        subtrees: [
          ...n.subtrees.slice(0, i),
          pushUp.leftSubtree,
          pushUp.rightSubtree,
          ...n.subtrees.slice(i + 1),
        ],
      });
    }
  } else {
    await ctx.db.patch(node, {
      items: [...n.items.slice(0, i), item, ...n.items.slice(i)],
    });
  }
  await ctx.db.patch(node, {
    aggregate: add(n.aggregate, itemAggregate(item)),
  });

  const newN = (await ctx.db.get(node))!;
  if (newN.items.length > MAX_NODE_SIZE(ctx)) {
    if (
      newN.items.length !== (MAX_NODE_SIZE(ctx)) + 1 ||
      newN.items.length !== 2 * MIN_NODE_SIZE(ctx) + 1
    ) {
      throw new Error(`bad ${newN.items.length}`);
    }
    log(`splitting node ${newN._id} at ${newN.items[MIN_NODE_SIZE(ctx)].k}`);
    const topLevel = nodeCounts(newN);
    const subCounts = await subtreeCounts(ctx.db, newN);
    const leftCount = add(
      accumulate(topLevel.slice(0, MIN_NODE_SIZE(ctx))),
      accumulate(subCounts.length ? subCounts.slice(0, MIN_NODE_SIZE(ctx) + 1) : []),
    );
    const rightCount = add(
      accumulate(topLevel.slice(MIN_NODE_SIZE(ctx) + 1)),
      accumulate(subCounts.length ? subCounts.slice(MIN_NODE_SIZE(ctx) + 1) : []),
    );
    if (leftCount.count + rightCount.count + 1 !== newN.aggregate.count) {
      throw new Error(
        `bad count split ${leftCount.count} ${rightCount.count} ${newN.aggregate.count}`
      );
    }
    if (leftCount.sum + rightCount.sum + newN.items[MIN_NODE_SIZE(ctx)].s !== newN.aggregate.sum) {
      throw new Error(
        `bad sum split ${leftCount.sum} ${rightCount.sum} ${newN.items[MIN_NODE_SIZE(ctx)].s} ${newN.aggregate.sum}`
      );
    }
    await ctx.db.patch(node, {
      items: newN.items.slice(0, MIN_NODE_SIZE(ctx)),
      subtrees: newN.subtrees.length
        ? newN.subtrees.slice(0, MIN_NODE_SIZE(ctx) + 1)
        : [],
      aggregate: leftCount,
    });
    const splitN = await ctx.db.insert("btreeNode", {
      items: newN.items.slice(MIN_NODE_SIZE(ctx) + 1),
      subtrees: newN.subtrees.length
        ? newN.subtrees.slice(MIN_NODE_SIZE(ctx) + 1)
        : [],
      aggregate: rightCount,
    });
    return {
      item: newN.items[MIN_NODE_SIZE(ctx)],
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

export async function getTree(db: DatabaseReader) {
  return await db
    .query("btree")
    .unique();
}

export async function mustGetTree(db: DatabaseReader) {
  const tree = await getTree(db);
  if (!tree) {
    throw new Error("btree not initialized");
  }
  return tree;
}

async function getOrCreateTree(
  db: DatabaseWriter,
  getKey: FunctionHandle<"query", { doc: Doc<"btreeNode"> }, { key: Key; summand?: number }>,
): Promise<Doc<"btree">> {
  const originalTree = await getTree(db);
  if (originalTree) {
    return originalTree;
  }
  const root = await db.insert("btreeNode", {
    items: [],
    subtrees: [],
    aggregate: {
      count: 0,
      sum: 0,
    },
  });
  const id = await db.insert("btree", {
    root,
    getKey,
  });
  const newTree = await db.get(id);
  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
  return newTree!;
}

export const init = mutation({
  args: { getKey: v.string() },
  handler: async (ctx, { getKey }) => {
    const existing = await ctx.db.query("btree").unique();
    if (existing) {
      await ctx.db.delete(existing._id);
    }
    await getOrCreateTree(ctx.db, getKey as any);
  },
});

export const clearTree = mutation({
  args: { },
  handler: async (ctx) => {
    const tree = await getTree(ctx.db);
    if (tree) {
      await ctx.db.delete(tree._id);
    }
  },
});

export const trigger = mutation({
  args: triggerArgsValidator(),
  returns: v.null(),
  handler: async (ctx, { change }) => {
    const tree = await mustGetTree(ctx.db);
    const getKey = tree.getKey as FunctionHandle<"query", { doc: GenericDocument }, { key: Key; summand?: number }>;
    switch (change.type) {
      case "insert": {
        const { key, summand } = await ctx.runQuery(getKey, { doc: change.newDoc! });
        await insertHandler(ctx, { key: [key, change.id], value: change.id, summand });
        break;
      }
      case "patch":
        // fallthrough
      case "replace": {
        const { key: keyBefore } = await ctx.runQuery(getKey, { doc: change.oldDoc! });
        const { key: keyAfter, summand: summandAfter } = await ctx.runQuery(getKey, { doc: change.newDoc! });
        await deleteHandler(ctx, { key: [keyBefore, change.id] });
        await insertHandler(ctx, { key: [keyAfter, change.id], value: change.id, summand: summandAfter });
        break;
      }
      case "delete": {
        const { key } = await ctx.runQuery(getKey, { doc: change.oldDoc! });
        await deleteHandler(ctx, { key: [key, change.id] });
        break;
      }
    }
  },
});
