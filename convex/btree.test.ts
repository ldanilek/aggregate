import { describe, expect, test } from "vitest";
import { convexTest } from "convex-test";
import schema from "./schema";
import { modules } from "./setup.test";
import {
  atIndexHandler,
  countBetweenHandler,
  deleteHandler,
  getHandler,
  insertHandler,
  rankHandler,
  validateTree,
} from "./btree";

describe("btree", () => {
  test("insert", async () => {
    const t = convexTest(schema, modules);
    await t.run(async (ctx) => {
      // Insert lots of keys. At each stage, the tree is valid.
      async function insert(key: number, value: string) {
        await insertHandler(ctx, { name: "foo", key, value });
        await validateTree(ctx, { name: "foo" });
        const get = await getHandler(ctx, { name: "foo", key });
        expect(get).toEqual({
          key,
          value,
        });
      }
      await insert(1, "a");
      await insert(4, "b");
      await insert(3, "c");
      await insert(2, "d");
      await insert(5, "e");
      await insert(6, "e");
      await insert(7, "e");
      await insert(10, "e");
      await insert(0, "e");
      await insert(-1, "e");
      await insert(9, "e");
      await insert(8, "e");
    });
  });

  test("delete", async () => {
    const t = convexTest(schema, modules);
    await t.run(async (ctx) => {
      async function insert(key: number, value: string) {
        await insertHandler(ctx, { name: "foo", key, value });
        await validateTree(ctx, { name: "foo" });
        const get = await getHandler(ctx, { name: "foo", key });
        expect(get).toEqual({
          key,
          value,
        });
      }
      // Delete keys. At each stage, the tree is valid.
      async function del(key: number) {
        await deleteHandler(ctx, { name: "foo", key });
        await validateTree(ctx, { name: "foo" });
        const get = await getHandler(ctx, { name: "foo", key });
        expect(get).toBeNull();
      }
      await insert(1, "a");
      await insert(2, "b");
      await del(1);
      await del(2);
      await insert(1, "a");
      await insert(2, "a");
      await insert(3, "c");
      await insert(4, "d");
      await insert(5, "e");
      await del(3);
      await insert(6, "e");
      await insert(7, "e");
      await insert(10, "e");
      await insert(0, "e");
      await insert(-1, "e");
      await insert(9, "e");
      await insert(8, "e");
      await del(-1);
      await del(6);
      await del(7);
      await del(0);
    });
  });

  test("atIndex and rank", async () => {
    const t = convexTest(schema, modules);
    await t.run(async (ctx) => {
      async function insert(key: number, value: string) {
        await insertHandler(ctx, { name: "foo", key, value });
        await validateTree(ctx, { name: "foo" });
        const rank = await rankHandler(ctx, { name: "foo", key });
        expect(rank).not.toBeNull();
        const atIndex = await atIndexHandler(ctx, {
          name: "foo",
          index: rank!,
        });
        expect(atIndex).toEqual({
          key,
          value,
        });
      }
      async function checkRank(key: number, rank: number) {
        const r = await rankHandler(ctx, { name: "foo", key });
        expect(r).toEqual(rank);
        const atIndex = await atIndexHandler(ctx, { name: "foo", index: rank });
        expect(atIndex.key).toEqual(key);
      }
      await insert(1, "a");
      await insert(4, "b");
      await insert(3, "c");
      await insert(2, "d");
      await insert(5, "e");
      await insert(6, "e");
      await insert(7, "e");
      await insert(10, "e");
      await insert(0, "e");
      await insert(-1, "e");
      await insert(9, "e");
      await insert(8, "e");
      await checkRank(-1, 0);
      await checkRank(10, 11);
      await checkRank(5, 6);
    });
  });

  test("countBetween", async () => {
    const t = convexTest(schema, modules);
    await t.run(async (ctx) => {
      async function insert(key: number, value: string) {
        await insertHandler(ctx, { name: "foo", key, value });
        await validateTree(ctx, { name: "foo" });
      }
      async function countBetween(
        k1: number | undefined,
        k2: number | undefined,
        count: number
      ) {
        const c = await countBetweenHandler(ctx, { name: "foo", k1, k2 });
        expect(c).toEqual(count);
      }
      await insert(0, "a");
      await insert(1, "a");
      await insert(2, "d");
      await insert(3, "c");
      await insert(4, "b");
      await insert(5, "e");
      await insert(6, "e");
      await insert(7, "e");
      await insert(8, "e");
      await insert(9, "e");
      await countBetween(-1, 10, 10);
      await countBetween(undefined, undefined, 10);
      await countBetween(4, 6, 1);
      await countBetween(0.5, 8.5, 8);
      await countBetween(6, 9, 2);
    });
  });
});
