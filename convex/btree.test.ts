import { describe, expect, test } from "vitest";
import { convexTest } from "convex-test";
import schema from "./schema";
import { modules } from "./setup.test";
import { dumpTree, insertHandler, validateTree } from "./btree";

describe("btree", () => {
  test("insert", async () => {
    const t = convexTest(schema, modules);
    await t.run(async (ctx) => {
      await insertHandler(ctx, { name: "foo", key: 1, value: "a" });
      await insertHandler(ctx, { name: "foo", key: 4, value: "b" });
      await insertHandler(ctx, { name: "foo", key: 3, value: "c" });
      await insertHandler(ctx, { name: "foo", key: 2, value: "d" });
      await insertHandler(ctx, { name: "foo", key: 5, value: "e" });
      console.log(await dumpTree(ctx.db, "foo"));
      await validateTree(ctx, { name: "foo" });
    });
  });
});
