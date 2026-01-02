/* eslint-disable no-process-env */
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import type {
  Checkpoint,
  CheckpointMetadata,
  CheckpointTuple,
} from "@langchain/langgraph-checkpoint";
import { emptyCheckpoint, TASKS, uuid6 } from "@langchain/langgraph-checkpoint";
import mysql from "mysql2/promise";
import { RunnableConfig } from "@langchain/core/runnables";
import { MysqlSaver } from "../index.js";
import { getMigrations } from "../migrations.js";

const checkpoint1: Checkpoint = {
  v: 1,
  id: uuid6(-1),
  ts: "2024-04-19T17:19:07.952Z",
  channel_values: {
    someKey1: "someValue1",
  },
  channel_versions: {
    someKey1: 1,
    someKey2: 1,
  },
  versions_seen: {
    someKey3: {
      someKey4: 1,
    },
  },
  // @ts-expect-error - older version of checkpoint
  pending_sends: [],
};

const checkpoint2: Checkpoint = {
  v: 1,
  id: uuid6(1),
  ts: "2024-04-20T17:19:07.952Z",
  channel_values: {
    someKey1: "someValue2",
  },
  channel_versions: {
    someKey1: 1,
    someKey2: 2,
  },
  versions_seen: {
    someKey3: {
      someKey4: 2,
    },
  },
  // @ts-expect-error - older version of checkpoint
  pending_sends: [],
};

const { TEST_MYSQL_URL } = process.env;
if (!TEST_MYSQL_URL) {
  throw new Error("TEST_MYSQL_URL environment variable is required");
}

describe("MysqlSaver", () => {
  let mysqlSaver: MysqlSaver;

  beforeEach(async () => {
    mysqlSaver = MysqlSaver.fromConnString(TEST_MYSQL_URL);
    await mysqlSaver.setup();
  });

  afterEach(async () => {
    // Clean up tables after each test
    try {
      const connection = await mysql.createConnection(TEST_MYSQL_URL);
      await connection.query("DROP TABLE IF EXISTS checkpoint_writes");
      await connection.query("DROP TABLE IF EXISTS checkpoint_blobs");
      await connection.query("DROP TABLE IF EXISTS checkpoints");
      await connection.query("DROP TABLE IF EXISTS checkpoint_migrations");
      await connection.end();
      await mysqlSaver.end();
    } catch (error) {
      console.error("Cleanup error:", error);
    }
  });

  it("should properly initialize and setup the database", async () => {
    // Verify that the database is properly initialized
    const connection = await mysql.createConnection(TEST_MYSQL_URL);
    try {
      // Check if the required tables exist
      const [tables] = await connection.query<mysql.RowDataPacket[]>(
        "SHOW TABLES"
      );
      const tableNames = tables.map((row) => Object.values(row)[0]);
      expect(tableNames).toContain("checkpoints");
      expect(tableNames).toContain("checkpoint_blobs");
      expect(tableNames).toContain("checkpoint_writes");
      expect(tableNames).toContain("checkpoint_migrations");

      // Verify migrations table has correct number of entries
      const [migrationsResult] = await connection.query<mysql.RowDataPacket[]>(
        "SELECT COUNT(*) as count FROM checkpoint_migrations"
      );
      const MIGRATIONS = getMigrations();
      const firstResult = migrationsResult[0];
      expect(firstResult).toBeDefined();
      expect(Number.parseInt(firstResult!.count, 10)).toBe(MIGRATIONS.length);
    } finally {
      await connection.end();
    }
  });

  it("should save and retrieve checkpoints correctly", async () => {
    // get undefined checkpoint
    const undefinedCheckpoint = await mysqlSaver.getTuple({
      configurable: { thread_id: "1" },
    });
    expect(undefinedCheckpoint).toBeUndefined();

    // save first checkpoint
    const runnableConfig = await mysqlSaver.put(
      { configurable: { thread_id: "1" } },
      checkpoint1,
      { source: "update", step: -1, parents: {} },
      checkpoint1.channel_versions
    );
    expect(runnableConfig).toEqual({
      configurable: {
        thread_id: "1",
        checkpoint_ns: "",
        checkpoint_id: checkpoint1.id,
      },
    });

    // add some writes
    await mysqlSaver.putWrites(
      {
        configurable: {
          checkpoint_id: checkpoint1.id,
          checkpoint_ns: "",
          thread_id: "1",
        },
      },
      [["bar", "baz"]],
      "foo"
    );

    // get first checkpoint tuple
    const firstCheckpointTuple = await mysqlSaver.getTuple({
      configurable: { thread_id: "1" },
    });
    expect(firstCheckpointTuple?.config).toEqual({
      configurable: {
        thread_id: "1",
        checkpoint_ns: "",
        checkpoint_id: checkpoint1.id,
      },
    });
    expect(firstCheckpointTuple?.checkpoint).toEqual(checkpoint1);
    expect(firstCheckpointTuple?.metadata).toEqual({
      source: "update",
      step: -1,
      parents: {},
    });
    expect(firstCheckpointTuple?.parentConfig).toBeUndefined();
    expect(firstCheckpointTuple?.pendingWrites).toEqual([
      ["foo", "bar", "baz"],
    ]);

    // save second checkpoint
    await mysqlSaver.put(
      {
        configurable: {
          thread_id: "1",
          checkpoint_id: "2024-04-18T17:19:07.952Z",
        },
      },
      checkpoint2,
      { source: "update", step: -1, parents: {} },
      checkpoint2.channel_versions
    );

    // verify that parentTs is set and retrieved correctly for second checkpoint
    const secondCheckpointTuple = await mysqlSaver.getTuple({
      configurable: { thread_id: "1" },
    });
    expect(secondCheckpointTuple?.metadata).toEqual({
      source: "update",
      step: -1,
      parents: {},
    });
    expect(secondCheckpointTuple?.parentConfig).toEqual({
      configurable: {
        thread_id: "1",
        checkpoint_ns: "",
        checkpoint_id: "2024-04-18T17:19:07.952Z",
      },
    });

    // list checkpoints
    const checkpointTupleGenerator = mysqlSaver.list({
      configurable: { thread_id: "1" },
    });
    const checkpointTuples: CheckpointTuple[] = [];
    for await (const checkpoint of checkpointTupleGenerator) {
      checkpointTuples.push(checkpoint);
    }
    expect(checkpointTuples.length).toBe(2);
    const checkpointTuple1 = checkpointTuples[0];
    const checkpointTuple2 = checkpointTuples[1];
    expect(checkpointTuple1).toBeDefined();
    expect(checkpointTuple2).toBeDefined();
    expect(checkpointTuple1!.checkpoint.ts).toBe("2024-04-20T17:19:07.952Z");
    expect(checkpointTuple2!.checkpoint.ts).toBe("2024-04-19T17:19:07.952Z");
  });

  it("should delete thread", async () => {
    const thread1 = { configurable: { thread_id: "1", checkpoint_ns: "" } };
    const thread2 = { configurable: { thread_id: "2", checkpoint_ns: "" } };

    const meta: CheckpointMetadata = {
      source: "update",
      step: -1,
      parents: {},
    };

    await mysqlSaver.put(thread1, emptyCheckpoint(), meta, {});
    await mysqlSaver.put(thread2, emptyCheckpoint(), meta, {});

    expect(await mysqlSaver.getTuple(thread1)).toBeDefined();

    await mysqlSaver.deleteThread("1");

    expect(await mysqlSaver.getTuple(thread1)).toBeUndefined();
    expect(await mysqlSaver.getTuple(thread2)).toBeDefined();
  });

  it("pending sends migration", async () => {
    let config: RunnableConfig = {
      configurable: { thread_id: "thread-1", checkpoint_ns: "" },
    };

    const checkpointZero: Checkpoint = {
      v: 1,
      id: uuid6(0),
      ts: "2024-04-19T17:19:07.952Z",
      channel_values: {},
      channel_versions: {},
      versions_seen: {},
    };

    config = await mysqlSaver.put(
      config,
      checkpointZero,
      { source: "loop", parents: {}, step: 0 },
      {}
    );

    await mysqlSaver.putWrites(
      config,
      [
        [TASKS, "send-1"],
        [TASKS, "send-2"],
      ],
      "task-1"
    );
    await mysqlSaver.putWrites(config, [[TASKS, "send-3"]], "task-2");

    // check that fetching checkpount 0 doesn't attach pending sends
    // (they should be attached to the next checkpoint)
    const tuple0 = await mysqlSaver.getTuple(config);
    expect(tuple0?.checkpoint.channel_values).toEqual({});
    expect(tuple0?.checkpoint.channel_versions).toEqual({});

    // create second checkpoint
    const checkpointOne: Checkpoint = {
      v: 1,
      id: uuid6(1),
      ts: "2024-04-20T17:19:07.952Z",
      channel_values: {},
      channel_versions: checkpointZero.channel_versions,
      versions_seen: checkpointZero.versions_seen,
      // @ts-expect-error - older version of checkpoint
      pending_sends: [],
    };

    config = await mysqlSaver.put(
      config,
      checkpointOne,
      { source: "loop", parents: {}, step: 1 },
      {}
    );

    // check that pending sends are attached to checkpoint1
    const checkpoint1Tuple = await mysqlSaver.getTuple(config);
    expect(checkpoint1Tuple?.checkpoint.channel_values).toEqual({
      [TASKS]: ["send-1", "send-2", "send-3"],
    });
    expect(checkpoint1Tuple?.checkpoint.channel_versions[TASKS]).toBeDefined();

    // check that the list also applies the migration
    const checkpointTupleGenerator = mysqlSaver.list({
      configurable: { thread_id: "thread-1" },
    });
    const checkpointTuples: CheckpointTuple[] = [];
    for await (const checkpoint of checkpointTupleGenerator) {
      checkpointTuples.push(checkpoint);
    }
    expect(checkpointTuples.length).toBe(2);
    const firstTuple = checkpointTuples[0];
    expect(firstTuple).toBeDefined();
    expect(firstTuple!.checkpoint.channel_values).toEqual({
      [TASKS]: ["send-1", "send-2", "send-3"],
    });
    expect(firstTuple!.checkpoint.channel_versions[TASKS]).toBeDefined();
  });
});
