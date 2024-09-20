import {
  DocumentByName,
  Expand,
  FunctionReference,
  GenericDataModel,
  GenericMutationCtx,
  GenericQueryCtx,
  TableNamesInDataModel,
  createFunctionHandle,
} from "convex/server";
import {
  Key,
} from "./btree";
import { api } from "./_generated/api";
import { GenericId } from "convex/values";

type InternalizeApi<API> = Expand<{
  [mod in keyof API]: API[mod] extends FunctionReference<any, any, any, any>
    ? FunctionReference<API[mod]["_type"], "internal", API[mod]["_args"], API[mod]["_returnType"], API[mod]["_componentPath"]>
    : InternalizeApi<API[mod]>;
}>;
type InstalledAPI = InternalizeApi<typeof api>;


export async function initBTree<
  DataModel extends GenericDataModel,
  T extends TableNamesInDataModel<DataModel>,
  K extends Key,
>(
  ctx: GenericMutationCtx<DataModel>,
  api: InstalledAPI, 
  getKey: FunctionReference<"query", any, { doc: DocumentByName<DataModel, T> }, {key: K; summand?: number}>,
): Promise<void> {
  await ctx.runMutation(api.btree.init, {
    getKey: await createFunctionHandle(getKey),
    maxNodeSize: 4,
  });
}

export class BTree<
  DataModel extends GenericDataModel,
  T extends TableNamesInDataModel<DataModel>,
  K extends Key,
> {
  constructor(
    private ctx: GenericQueryCtx<DataModel>,
    private api: InstalledAPI,
  ) {
  }
  async get(key: K): Promise<GenericId<T> | null> {
    const item = await this.ctx.runQuery(this.api.btree.get, { key });
    if (item === null) {
      return null;
    }
    return item.v as GenericId<T>;
  }
  async at(index: number): Promise<{ k: K; v: GenericId<T>; s: number }> {
    const item = await this.ctx.runQuery(this.api.btree.atIndex, { index });
    return item as { k: K; v: GenericId<T>; s: number };
  }
  async indexOf(key: K): Promise<number | null> {
    return await this.ctx.runQuery(this.api.btree.rank, { key });
  }
  async count(): Promise<number> {
    return await this.ctx.runQuery(this.api.btree.count, { });
  }
  async sum(): Promise<number> {
    return await this.ctx.runQuery(this.api.btree.sum, { });
  }
  async min(): Promise<{ k: K; v: GenericId<T>; s: number } | null> {
    const count = await this.count();
    if (count === 0) {
      return null;
    }
    return await this.at(0);
  }
  async max(): Promise<{ k: K; v: GenericId<T>; s: number } | null> {
    const count = await this.count();
    if (count === 0) {
      return null;
    }
    return await this.at(count - 1);
  }
  // Count keys strictly between k1 and k2.
  async countBetween(k1: K, k2: K): Promise<number> {
    const { count } = await this.ctx.runQuery(this.api.btree.countBetween, { k1, k2 });
    return count;
  }
  async random(): Promise<{ k: K; v: GenericId<T>; s: number } | null> {
    const count = await this.count();
    if (count === 0) {
      return null;
    }
    const index = Math.floor(Math.random() * count);
    return await this.at(index);
  }
  async validate(): Promise<void> {
    await this.ctx.runQuery(this.api.btree.validate, {});
  }
  // TODO: iter between keys
}
