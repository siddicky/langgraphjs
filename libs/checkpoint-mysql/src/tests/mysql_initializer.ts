import type { CheckpointerTestInitializer } from "@langchain/langgraph-checkpoint-validation";
import {
  MySqlContainer,
  type StartedMySqlContainer,
} from "@testcontainers/mysql";
import mysql from "mysql2/promise";

import { MysqlSaver } from "../index.js";

const dbName = "test_db";

const container = new MySqlContainer("mysql:8.0")
  .withDatabase("mysql")
  .withRootPassword("password");

let startedContainer: StartedMySqlContainer;
let connection: mysql.Connection | undefined;

export const initializer: CheckpointerTestInitializer<MysqlSaver> = {
  checkpointerName: "@langchain/langgraph-checkpoint-mysql",

  async beforeAll() {
    startedContainer = await container.start();
  },

  beforeAllTimeout: 300_000, // five minutes, to pull docker container

  async afterAll() {
    await startedContainer.stop();
  },

  async createCheckpointer() {
    connection = await mysql.createConnection({
      host: startedContainer.getHost(),
      port: startedContainer.getPort(),
      user: startedContainer.getUsername(),
      password: startedContainer.getRootPassword(),
    });

    await connection.query(`CREATE DATABASE IF NOT EXISTS \`${dbName}\``);
    await connection.query(`USE \`${dbName}\``);

    const connString = `mysql://${startedContainer.getUsername()}:${startedContainer.getRootPassword()}@${startedContainer.getHost()}:${startedContainer.getPort()}/${dbName}`;

    const checkpointer = MysqlSaver.fromConnString(connString);
    await checkpointer.setup();
    return checkpointer;
  },

  async destroyCheckpointer(checkpointer: MysqlSaver) {
    await checkpointer.end();
    if (connection) {
      await connection.query(`DROP DATABASE IF EXISTS \`${dbName}\``);
      await connection.end();
    }
  },
};
