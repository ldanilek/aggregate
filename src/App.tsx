import { Button } from "@/components/ui/button";
import { useMutation, useQuery } from "convex/react";
import { api } from "../convex/_generated/api";
import { Link } from "@/components/typography/link";

function App() {
  const numbers = useQuery(api.myFunctions.listNumbers, { count: 10 });
  const numberCount = useQuery(api.btree.count, { name: "numbers" }) ?? 0;
  const addNumber = useMutation(api.myFunctions.addNumber);
  
  const numbersByIndex = [];
  for (let i = 0; i < numberCount; i++) {
    numbersByIndex.push(<NumberByIndex key={i} i={i} />);
  }

  return (
    <main className="container max-w-2xl flex flex-col gap-8">
      <h1 className="text-4xl font-extrabold my-8 text-center">
        Convex + React (Vite)
      </h1>
      <p>
        Click the button and open this page in another window - this data is
        persisted in the Convex cloud database!
      </p>
      <p>
        <Button
          onClick={() => {
            void addNumber({ value: Math.floor(Math.random() * 100) });
          }}
        >
          Add a random number
        </Button>
      </p>
      <p>
        Numbers:{" "}
        {numbers?.length === 0
          ? "Click the button!"
          : numbers?.join(", ") ?? "..."}
      </p>
      <div>
        Numbers by index:{" "}
        {numbersByIndex}
      </div>
      <p>
        Edit{" "}
        <code className="relative rounded bg-muted px-[0.3rem] py-[0.2rem] font-mono text-sm font-semibold">
          convex/myFunctions.ts
        </code>{" "}
        to change your backend
      </p>
      <p>
        Edit{" "}
        <code className="relative rounded bg-muted px-[0.3rem] py-[0.2rem] font-mono text-sm font-semibold">
          src/App.tsx
        </code>{" "}
        to change your frontend
      </p>
      <p>
        Check out{" "}
        <Link target="_blank" href="https://docs.convex.dev/home">
          Convex docs
        </Link>
      </p>
    </main>
  );
}

function NumberByIndex({i}: {i: number}) {
  const n = useQuery(api.btree.atIndex, { name: "numbers", index: i });
  const removeNumber = useMutation(api.myFunctions.removeNumber);

  if (!n) return <p>Loading...</p>;

  return <p>
    Number at index {i} is {n.key}. <button onClick={() => {
      void removeNumber({ number: n.value });
    }}>delete</button>
  </p>
}

export default App;
