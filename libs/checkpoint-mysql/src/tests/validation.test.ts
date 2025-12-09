import { validate } from "@langchain/langgraph-checkpoint-validation";
import { initializer } from "./mysql_initializer.js";

validate(initializer);
