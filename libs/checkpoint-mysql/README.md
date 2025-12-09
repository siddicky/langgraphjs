# @langchain/langgraph-checkpoint-mysql

LangGraph checkpointer that uses MySQL as the backing store.

## Usage

```typescript
import { ChatOpenAI } from "@langchain/openai";
import { MysqlSaver } from "@langchain/langgraph-checkpoint-mysql";
import { createReactAgent } from "@langchain/langgraph/prebuilt";

const checkpointer = MysqlSaver.fromConnString(
  "mysql://user:password@localhost:3306/mydb"
);

// NOTE: you need to call .setup() the first time you're using your checkpointer
await checkpointer.setup();

const graph = createReactAgent({
  tools: [getWeather],
  llm: new ChatOpenAI({
    model: "gpt-4o-mini",
  }),
  checkpointSaver: checkpointer,
});

const config = { configurable: { thread_id: "1" } };

await graph.invoke({
  messages: [{
    role: "user",
    content: "what's the weather in sf"
  }],
}, config);
```

## License

MIT
