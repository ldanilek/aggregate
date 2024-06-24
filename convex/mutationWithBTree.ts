
import { customMutation } from "convex-helpers/server/customFunctions";
import { DatabaseReader, mutation } from "./_generated/server";
import { DocumentByName, GenericDataModel, GenericDatabaseWriter, QueryInitializer, TableNamesInDataModel } from "convex/server";
import { Key, atIndexHandler, countBetweenHandler, countHandler, deleteHandler, getHandler, insertHandler, rankHandler } from "./btree";
import { GenericId } from "convex/values";

export type AttachBTree<DataModel extends GenericDataModel, T extends TableNamesInDataModel<DataModel>, K extends Key> = {
  tableName: T;
  btreeName: string;
  getKey: (doc: DocumentByName<DataModel, T>) => K;
};

export function mutationWithBTree<
  DataModel extends GenericDataModel,
  T extends TableNamesInDataModel<DataModel>,
  K extends Key,
>(
  btree: AttachBTree<DataModel, T, K>,
) {
  return customMutation(mutation, {
    args: {},
    input: async (ctx, _args) => {
      const db = new WrapWriter(ctx.db, btree);
      return { ctx: { db }, args: {} };
    },
  });
}

export class BTree<
  DataModel extends GenericDataModel,
  T extends TableNamesInDataModel<DataModel>,
  K extends Key,
> {
  ctx: { db: DatabaseReader };
  name: string;

  constructor(ctx: { db: DatabaseReader }, name: string) {
    this.ctx = ctx;
    this.name = name;
  }
  async get(key: K): Promise<GenericId<T> | null> {
    const keyValue = await getHandler(this.ctx, { name: this.name, key });
    if (keyValue === null) {
      return null;
    }
    return keyValue.value as GenericId<T>;
  }
  async at(index: number): Promise<{ key: K; value: GenericId<T> }> {
    const keyValue = await atIndexHandler(this.ctx, { name: this.name, index });
    return keyValue as { key: K; value: GenericId<T> };
  }
  async indexOf(key: K): Promise<number | null> {
    return await rankHandler(this.ctx, { name: this.name, key });
  }
  async count(): Promise<number> {
    return await countHandler(this.ctx, { name: this.name });
  }
  async min(): Promise<{ key: K; value: GenericId<T> } | null> {
    const count = await this.count();
    if (count === 0) {
      return null;
    }
    return await this.at(0);
  }
  async max(): Promise<{ key: K; value: GenericId<T> } | null> {
    const count = await this.count();
    if (count === 0) {
      return null;
    }
    return await this.at(count - 1);
  }
  async countBetween(k1: K, k2: K): Promise<number> {
    return await countBetweenHandler(this.ctx, { name: this.name, k1, k2 });
  }
}
// TODO: count between
// TODO: iter between


class WrapWriter<
  DataModel extends GenericDataModel,
  T extends TableNamesInDataModel<DataModel>,
  K extends Key,
>
{
  db: GenericDatabaseWriter<DataModel>;
  system: GenericDatabaseWriter<DataModel>["system"];
  btree: AttachBTree<DataModel, T, K>;

  constructor(
    db: GenericDatabaseWriter<DataModel>,
    btree: AttachBTree<DataModel, T, K>,
  ) {
    this.db = db;
    this.system = db.system;
    this.btree = btree;
  }
  normalizeId<TableName extends TableNamesInDataModel<DataModel>>(
    tableName: TableName,
    id: string
  ): GenericId<TableName> | null {
    return this.db.normalizeId(tableName, id);
  }
  async insert<TableName extends string>(
    table: TableName,
    value: any
  ): Promise<any> {
    const id = await this.db.insert(table, value);
    if (table === this.btree.tableName as any) {
      await insertHandler({ db: this.db }, {
        name: this.btree.btreeName,
        key: this.btree.getKey(value),
        value: id,
      });
    }
    return id;
  }
  async patch<TableName extends string>(
    tableName: TableName,
    id: GenericId<TableName>,
    value: Partial<any>
  ): Promise<void> {
    if (tableName === this.btree.tableName as any) {
      const keyBefore = this.btree.getKey((await this.db.get(id))!);
      await deleteHandler({ db: this.db }, {
        name: this.btree.btreeName,
        key: keyBefore,
      });
    }
    await this.db.patch(id, value);
    if (tableName === this.btree.tableName as any) {
      const keyAfter = this.btree.getKey((await this.db.get(id))!);
      await insertHandler({ db: this.db }, {
        name: this.btree.btreeName,
        key: keyAfter,
        value: id,
      });
    }
  }
  async replace<TableName extends string>(
    tableName: TableName,
    id: GenericId<TableName>,
    value: any
  ): Promise<void> {
    if (tableName === this.btree.tableName as any) {
      const keyBefore = this.btree.getKey((await this.db.get(id))!);
      await deleteHandler({ db: this.db }, {
        name: this.btree.btreeName,
        key: keyBefore,
      });
    }
    await this.db.replace(id, value);
    if (tableName === this.btree.tableName as any) {
      const keyAfter = this.btree.getKey((await this.db.get(id))!);
      await insertHandler({ db: this.db }, {
        name: this.btree.btreeName,
        key: keyAfter,
        value: id,
      });
    }
  }
  async delete(tableName: any, id: GenericId<string>): Promise<void> {
    if (tableName === this.btree.tableName as any) {
      const keyBefore = this.btree.getKey((await this.db.get(id))!);
      await deleteHandler({ db: this.db }, {
        name: this.btree.btreeName,
        key: keyBefore,
      });
    }
    return await this.db.delete(id);
  }
  get<TableName extends string>(id: GenericId<TableName>): Promise<any> {
    return this.db.get(id);
  }
  query<TableName extends string>(tableName: TableName): QueryInitializer<any> {
    return this.db.query(tableName);
  }
}
