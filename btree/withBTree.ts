import {
  DocumentByName,
  Expand,
  FilterApi,
  FunctionReference,
  GenericDataModel,
  GenericDatabaseWriter,
  GenericMutationCtx,
  GenericQueryCtx,
  QueryInitializer,
  TableNamesInDataModel,
} from "convex/server";
import {
  Key,
} from "./btree";
import { functions } from "./_generated/api";
import { GenericId } from "convex/values";

type InternalizeApi<API> = Expand<{
  [mod in keyof API]: API[mod] extends FunctionReference<any, any, any, any>
    ? FunctionReference<API[mod]["_type"], "internal", API[mod]["_args"], API[mod]["_returnType"], API[mod]["_componentPath"]>
    : InternalizeApi<API[mod]>;
}>;
type InstalledAPI = InternalizeApi<
  FilterApi<typeof functions, FunctionReference<any, "public", any, any>>>;


export function mutationWithBTree<
  DataModel extends GenericDataModel,
  T extends TableNamesInDataModel<DataModel>,
  K extends Key,
>(btree: AttachBTree<DataModel, T, K>) {
  return {
    args: {},
    input: async (ctx: GenericMutationCtx<DataModel>, _args: any) => {
      const db = new WrapWriter(ctx, btree);
      return {
        ctx: { db },
        args: {},
      };
    },
  };
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

class WrapWriter<
  DataModel extends GenericDataModel,
  T extends TableNamesInDataModel<DataModel>,
  K extends Key,
> {
  ctx: GenericMutationCtx<DataModel>;
  system: GenericDatabaseWriter<DataModel>["system"];
  btree: AttachBTree<DataModel, T, K>;

  constructor(
    ctx: GenericMutationCtx<DataModel>,
    btree: AttachBTree<DataModel, T, K>
  ) {
    this.ctx = ctx;
    this.system = ctx.db.system;
    this.btree = btree;
  }
  normalizeId<TableName extends TableNamesInDataModel<DataModel>>(
    tableName: TableName,
    id: string
  ): GenericId<TableName> | null {
    return this.ctx.db.normalizeId(tableName, id);
  }
  async insert<TableName extends string>(
    table: TableName,
    value: any
  ): Promise<any> {
    const id = await this.ctx.db.insert(table, value);
    if (table === (this.btree.tableName as any)) {
      await this.ctx.runMutation(this.btree.api.btree.insert, {
        key: this.btree.getKey(value),
        value: id,
        summand: this.btree.getSummand(value),
      });
    }
    return id;
  }
  async patch<TableName extends string>(
    tableName: TableName,
    id: GenericId<TableName>,
    value: Partial<any>
  ): Promise<void> {
    if (tableName === (this.btree.tableName as any)) {
      // WARNING: these four operations get, patch, get, modifyKey are not atomic.
      // If called in parallel with other writes, the tree may become invalid.
      const keyBefore = this.btree.getKey((await this.ctx.db.get(id))!);
      await this.ctx.db.patch(id, value);
      const valueAfter = (await this.ctx.db.get(id))!;
      const keyAfter = this.btree.getKey(valueAfter);

      await this.ctx.runMutation(this.btree.api.btree.modifyKey,
        {
          keyBefore,
          keyAfter,
          value: id,
          summand: this.btree.getSummand(valueAfter),
        }
      );
    } else {
      await this.ctx.db.patch(id, value);
    }
  }
  async replace<TableName extends string>(
    tableName: TableName,
    id: GenericId<TableName>,
    value: any
  ): Promise<void> {
    if (tableName === (this.btree.tableName as any)) {
      const keyBefore = this.btree.getKey((await this.ctx.db.get(id))!);
      await this.ctx.db.replace(id, value);
      const valueAfter = (await this.ctx.db.get(id))!;
      const keyAfter = this.btree.getKey(valueAfter);
      await this.ctx.runMutation(this.btree.api.btree.modifyKey,
        {
          keyBefore,
          keyAfter,
          value: id,
          summand: this.btree.getSummand(valueAfter),
        }
      );
    } else {
      await this.ctx.db.replace(id, value);
    }
  }
  async delete(tableName: any, id: GenericId<string>): Promise<void> {
    if (tableName === (this.btree.tableName as any)) {
      const keyBefore = this.btree.getKey((await this.ctx.db.get(id))!);
      await this.ctx.runMutation(this.btree.api.btree.deleteKey,
        {
          key: keyBefore,
        }
      );
    }
    return await this.ctx.db.delete(id);
  }
  get<TableName extends string>(id: GenericId<TableName>): Promise<any> {
    return this.ctx.db.get(id);
  }
  query<TableName extends string>(tableName: TableName): QueryInitializer<any> {
    return this.ctx.db.query(tableName);
  }
}

export type AttachBTree<
  DataModel extends GenericDataModel,
  T extends TableNamesInDataModel<DataModel>,
  K extends Key,
> = {
  tableName: T;
  api: InstalledAPI;
  getKey: (doc: DocumentByName<DataModel, T>) => K;
  getSummand: (doc: DocumentByName<DataModel, T>) => number;
};
