
import { customMutation } from "convex-helpers/server/customFunctions";
import { mutation } from "./_generated/server";
import { DocumentByName, GenericDataModel, GenericDatabaseWriter, QueryInitializer, TableNamesInDataModel } from "convex/server";
import { Key, deleteHandler, insertHandler } from "./btree";
import { GenericId } from "convex/values";

export type AttachBTree<DataModel extends GenericDataModel, T extends TableNamesInDataModel<DataModel>> = {
  tableName: T;
  btreeName: string;
  getKey: (doc: DocumentByName<DataModel, T>) => Key;
};

export function mutationWithBTree<DataModel extends GenericDataModel, T extends TableNamesInDataModel<DataModel>>(
  btree: AttachBTree<DataModel, T>,
) {
  return customMutation(mutation, {
  args: {},
  input: async (ctx, _args) => {
    const db = new WrapWriter(ctx.db, btree);
    return { ctx: { db }, args: {} };
  },
});
}

class WrapWriter<DataModel extends GenericDataModel, T extends TableNamesInDataModel<DataModel>>
{
  db: GenericDatabaseWriter<DataModel>;
  system: GenericDatabaseWriter<DataModel>["system"];
  btree: AttachBTree<DataModel, T>;

  constructor(
    db: GenericDatabaseWriter<DataModel>,
    btree: AttachBTree<DataModel, T>,
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
