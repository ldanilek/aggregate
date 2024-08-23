import { defineApp } from "convex/server";

import btree from "../btree/component.config";
import triggers from "../triggers/component.config";

const app = defineApp();
const _btree = app.install(btree, {
    name: "numbersBTree",
    args: {},
});
app.install(triggers, {});

export default app;
