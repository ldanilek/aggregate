import { Button } from "@/components/ui/button";
import { useMutation, useQuery } from "convex/react";
import { api } from "../convex/_generated/api";
import { Id } from "../convex/_generated/dataModel";
import { useState } from "react";

function App() {
  const numbers = useQuery(api.myFunctions.listNumbers, { count: 10 });
  const numberCount = useQuery(api.btree.count, { name: "numbers" }) ?? 0;
  const addNumber = useMutation(api.myFunctions.addNumber);
  const [randomIndex, setRandomIndex] = useState(Math.floor(Math.random() * numberCount));

  
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
      {false && 
      <div>
        Numbers by index:{" "}
        {numbersByIndex}
      </div>
      }
      <p>Let's look up a random index! <button onClick={() => {
        setRandomIndex(Math.floor(Math.random() * numberCount));
      }}>reroll</button></p>
      <div>{
        numberCount && <NumberByIndex i={randomIndex} />
      }</div>
    </main>
  );
}

function NumberByIndex({i}: {i: number}) {
  const n = useQuery(api.myFunctions.numberAtIndex, { index: i });
  const removeNumber = useMutation(api.myFunctions.removeNumber);

  if (!n) return <p>Loading...</p>;

  return <p>
    Number at index {i} is {n.key?.toString()}. <button onClick={() => {
      void removeNumber({ number: n.value as Id<"numbers"> });
    }}>delete</button>
  </p>
}

export default App;
