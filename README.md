# Convex with BTrees

I heard you like BTrees. So I made a BTree on your BTree.

This library implements a custom BTree with Convex documents to index
another Convex table. Each node is annotated with a count, which
allows some common operations to be done easily.

See the `class BTree` methods for operations you can do.

1. Get an item at an offset with `.at(i)`, e.g. jump to the 5000th image in your photo album.
2. Get the offset of an item with `indexOf(k)`, e.g. if each player has a score, determine that a specific player is ranked 4245th in the leaderboard.
3. Count documents in the table.
4. Count documents between some range of keys in the table.
5. Get the max or min or documents in the table.
6. Get a uniformly random document from the table.

## How to use

This library uses Convex's internal Components framework, so it cannot be used
yet by outside projects.

Old installation instructions:

NOTE: once Convex ships the upcoming "components" project, installation
will hopefully become simpler.

1. Copy the `btree/` directory into your `convex/` folder.
2. Add the `btree` and `btreeNode` tables to your schema.
3. Using the pattern from `myFunctions.ts`, call `mutationWithBTree` and `queryWithBTree` and use those custom queries and mutations in place of normal `query` and `mutation`.
   - Replacing usages of normal `mutation` is important, because it allows the BTree implementation to track changes to the table.
4. Access the BTree methods as `ctx.tree`.
5. Change usages of `ctx.db` mutation methods that only take in an ID to also take in a table name, like `ctx.db.delete(messageId)` -> `ctx.db.delete("messages", messageId)`.

If you have existing data in the table, you will need to populate the BTree so it can track existing documents. You can define and call a method like the example `backfillBTree`.

Note that modifying the table in the dashboard will not update the BTree. To keep the BTree in sync with the table, you must use the custom mutations from step 3.

## Feedback

This library is a work-in-progress.

Feel free to leave issues on Github or ask about the library in Convex's Discord.
