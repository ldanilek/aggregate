import {
  DocumentByName,
  GenericDataModel,
  GenericDatabaseReader,
  GenericDatabaseWriter,
  GenericMutationCtx,
  GenericQueryCtx,
  QueryInitializer,
  TableNamesInDataModel,
} from "convex/server";
import {
  Key,
} from "./btree";
import { app } from "../convex/_generated/server";
import { GenericId } from "convex/values";

export function queryWithBTree<
  DataModel extends GenericDataModel,
  T extends TableNamesInDataModel<DataModel>,
  K extends Key,
  Name extends string,
>(btree: AttachBTree<DataModel, T, K, Name>) {
  return {
    args: {},
    input: async (ctx: any, _args: any) => {
      const tree = new BTree<DataModel, T, K>(ctx, btree.btreeName);
      // TODO: figure out how to attach it with a dynamic Name.
      return {
        ctx: { tree },
        args: {},
      } as const;
    },
  } as const;
}

export function mutationWithBTree<
  DataModel extends GenericDataModel,
  T extends TableNamesInDataModel<DataModel>,
  K extends Key,
  Name extends string,
>(btree: AttachBTree<DataModel, T, K, Name>) {
  return {
    args: {},
    input: async (ctx: GenericMutationCtx<DataModel>, _args: any) => {
      const db = new WrapWriter(ctx, btree);
      const tree = new BTree<DataModel, T, K>(ctx, btree.btreeName);
      return {
        ctx: { db, tree },
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
  ctx: { db: GenericDatabaseReader<DataModel> };
  name: string;

  constructor(ctx: GenericQueryCtx<DataModel>, name: string) {
    this.ctx = ctx;
    this.name = name;
  }
  async get(key: K): Promise<GenericId<T> | null> {
    // @ts-ignore
    const item = await this.ctx.runQuery(app.btree.btree.get, { name: this.name, key });
    if (item === null) {
      return null;
    }
    return item.v as GenericId<T>;
  }
  async at(index: number): Promise<{ k: K; v: GenericId<T>; s: number }> {
    // @ts-ignore
    const item = await this.ctx.runQuery(app.btree.btree.atIndex, { name: this.name, index });
    return item as { k: K; v: GenericId<T>; s: number };
  }
  async indexOf(key: K): Promise<number | null> {
    // @ts-ignore
    return await this.ctx.runQuery(app.btree.btree.rank, { name: this.name, key });
  }
  async count(): Promise<number> {
    // @ts-ignore
    return await this.ctx.runQuery(app.btree.btree.count, { name: this.name });
  }
  async sum(): Promise<number> {
    // @ts-ignore
    return await this.ctx.runQuery(app.btree.btree.sum, { name: this.name });
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
    // @ts-ignore
    return await this.ctx.runQuery(app.btree.btree.countBetween, { name: this.name, k1, k2 });
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
    // @ts-ignore
    await this.ctx.runQuery(app.btree.btree.validate, { name: this.name });
  }
  // TODO: iter between keys
}

class WrapWriter<
  DataModel extends GenericDataModel,
  T extends TableNamesInDataModel<DataModel>,
  K extends Key,
  Name extends string,
> {
  ctx: GenericMutationCtx<DataModel>;
  system: GenericDatabaseWriter<DataModel>["system"];
  btree: AttachBTree<DataModel, T, K, Name>;

  constructor(
    ctx: GenericMutationCtx<DataModel>,
    btree: AttachBTree<DataModel, T, K, Name>
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
      // @ts-ignore
      await this.ctx.runMutation(app.btree.btree.insert, {
        name: this.btree.btreeName,
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
      const keyBefore = this.btree.getKey((await this.ctx.db.get(id))!);
      // @ts-ignore
      await this.ctx.runMutation(app.btree.btree.deleteKey,
        {
          name: this.btree.btreeName,
          key: keyBefore,
        }
      );
    }
    await this.ctx.db.patch(id, value);
    if (tableName === (this.btree.tableName as any)) {
      const valueAfter = (await this.ctx.db.get(id))!;
      const keyAfter = this.btree.getKey(valueAfter);
      // @ts-ignore
      await this.ctx.runMutation(app.btree.btree.insert,
        {
          name: this.btree.btreeName,
          key: keyAfter,
          value: id,
          summand: this.btree.getSummand(valueAfter),
        }
      );
    }
  }
  async replace<TableName extends string>(
    tableName: TableName,
    id: GenericId<TableName>,
    value: any
  ): Promise<void> {
    if (tableName === (this.btree.tableName as any)) {
      const keyBefore = this.btree.getKey((await this.ctx.db.get(id))!);
      // @ts-ignore
      await this.ctx.runMutation(app.btree.btree.deleteKey,
        {
          name: this.btree.btreeName,
          key: keyBefore,
        }
      );
    }
    await this.ctx.db.replace(id, value);
    if (tableName === (this.btree.tableName as any)) {
      const valueAfter = (await this.ctx.db.get(id))!;
      const keyAfter = this.btree.getKey(valueAfter);
      // @ts-ignore
      await this.ctx.runMutation(app.btree.btree.insert,
        {
          name: this.btree.btreeName,
          key: keyAfter,
          value: id,
          summand: this.btree.getSummand(valueAfter),
        }
      );
    }
  }
  async delete(tableName: any, id: GenericId<string>): Promise<void> {
    if (tableName === (this.btree.tableName as any)) {
      const keyBefore = this.btree.getKey((await this.ctx.db.get(id))!);
      // @ts-ignore
      await this.ctx.runMutation(app.btree.btree.deleteKey,
        {
          name: this.btree.btreeName,
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
  Name extends string,
> = {
  tableName: T;
  btreeName: Name;
  getKey: (doc: DocumentByName<DataModel, T>) => K;
  getSummand: (doc: DocumentByName<DataModel, T>) => number;
};
