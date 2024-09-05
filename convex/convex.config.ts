import { defineApp } from "convex/server";

import btree from "../btree/convex.config";
import triggers from "../triggers/convex.config";

const app = defineApp();
app.use(btree, {
    name: "numbersBTree",
});
app.use(triggers, {});

export default app;
