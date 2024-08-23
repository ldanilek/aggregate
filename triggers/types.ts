
import {
  DocumentByName,
  GenericDataModel,
  TableNamesInDataModel,
} from "convex/server";
import { Infer, v, Validator } from "convex/values";

export type TriggerArgs<DataModel extends GenericDataModel, TableName extends TableNamesInDataModel<DataModel>> = {
  change: Infer<ReturnType<typeof triggerArgsValidator<DataModel, TableName>>["change"]>
};

export function triggerArgsValidator<DataModel extends GenericDataModel, TableName extends TableNamesInDataModel<DataModel>>() {
  return {
    change: v.object({
      type: v.union(v.literal("insert"), v.literal("patch"), v.literal("replace"), v.literal("delete")),
      id: v.string(), // as Validator<GenericId<TableName>>,
      oldDoc: v.any() as Validator<DocumentByName<DataModel, TableName> | null>,
      newDoc: v.any() as Validator<DocumentByName<DataModel, TableName> | null>,
    }),
  };
}
