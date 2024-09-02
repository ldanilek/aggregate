import { defineApp } from "convex/server";

import btree from "../btree/convex.config";
import triggers from "../triggers/convex.config";

const app = defineApp();
const _btree = app.install(btree, {
    name: "numbersBTree",
    args: {},
});
app.install(triggers, {});

export default app;
