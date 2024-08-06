import { defineApp } from "convex/server";

import btree from "../btree/component.config";

const app = defineApp();
const _btree = app.install(btree, {
    name: "numbersBTree",
    args: {},
});

export default app;
