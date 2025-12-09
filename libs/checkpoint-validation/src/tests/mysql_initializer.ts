/* eslint-disable import/no-extraneous-dependencies */
import { MysqlSaver } from "@langchain/langgraph-checkpoint-mysql";
import {
  MySqlContainer,
  type StartedMySqlContainer,
} from "@testcontainers/mysql";

import mysql from "mysql2/promise";
import type { CheckpointerTestInitializer } from "../types.js";

const dbName = "test_db";

const container = new MySqlContainer("mysql:8.0")
  .withDatabase(dbName)
  .withUsername("testuser")
  .withUserPassword("testpass")
  .withRootPassword("rootpass");

let startedContainer: StartedMySqlContainer;
let client: mysql.Connection | undefined;

function getConnectionString(): string {
  return `mysql://${startedContainer.getUsername()}:${startedContainer.getUserPassword()}@${startedContainer.getHost()}:${startedContainer.getPort()}/${dbName}`;
}

export const initializer: CheckpointerTestInitializer<MysqlSaver> = {
  checkpointerName: "@langchain/langgraph-checkpoint-mysql",

  async beforeAll() {
    startedContainer = await container.start();
  },

  beforeAllTimeout: 300_000, // five minutes, to pull docker container

  async afterAll() {
    await client?.end();
    await startedContainer.stop();
  },

  async createCheckpointer() {
    const connectionString = getConnectionString();

    const checkpointer = MysqlSaver.fromConnString(connectionString);
    await checkpointer.setup();
    return checkpointer;
  },

  async destroyCheckpointer(checkpointer: MysqlSaver) {
    // Clean up tables
    client = await mysql.createConnection(getConnectionString());

    await client?.query("DROP TABLE IF EXISTS checkpoint_writes");
    await client?.query("DROP TABLE IF EXISTS checkpoint_blobs");
    await client?.query("DROP TABLE IF EXISTS checkpoints");
    await client?.query("DROP TABLE IF EXISTS checkpoint_migrations");

    await checkpointer.end();
    await client?.end();
  },
};

export default initializer;
