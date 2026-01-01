import type { RunnableConfig } from "@langchain/core/runnables";
import {
  BaseCheckpointSaver,
  type Checkpoint,
  type CheckpointListOptions,
  type CheckpointTuple,
  type SerializerProtocol,
  type PendingWrite,
  type CheckpointMetadata,
  type ChannelVersions,
  WRITES_IDX_MAP,
  TASKS,
  maxChannelVersion,
} from "@langchain/langgraph-checkpoint";
import mysql from "mysql2/promise";

import { getMigrations } from "./migrations.js";
import {
  type SQL_STATEMENTS,
  type SQL_TYPES,
  getSQLStatements,
} from "./sql.js";

/**
 * LangGraph checkpointer that uses a MySQL instance as the backing store.
 * Uses the [mysql2](https://github.com/sidorares/node-mysql2) package internally
 * to connect to a MySQL instance.
 *
 * @example
 * ```
 * import { ChatOpenAI } from "@langchain/openai";
 * import { MysqlSaver } from "@langchain/langgraph-checkpoint-mysql";
 * import { createReactAgent } from "@langchain/langgraph/prebuilt";
 *
 * const checkpointer = MysqlSaver.fromConnString(
 *   "mysql://user:password@localhost:3306/db"
 * );
 *
 * // NOTE: you need to call .setup() the first time you're using your checkpointer
 * await checkpointer.setup();
 *
 * const graph = createReactAgent({
 *   tools: [getWeather],
 *   llm: new ChatOpenAI({
 *     model: "gpt-4o-mini",
 *   }),
 *   checkpointSaver: checkpointer,
 * });
 * const config = { configurable: { thread_id: "1" } };
 *
 * await graph.invoke({
 *   messages: [{
 *     role: "user",
 *     content: "what's the weather in sf"
 *   }],
 * }, config);
 * ```
 */
export class MysqlSaver extends BaseCheckpointSaver {
  private readonly pool: mysql.Pool;

  private readonly SQL_STATEMENTS: SQL_STATEMENTS;

  protected isSetup: boolean;

  constructor(pool: mysql.Pool, serde?: SerializerProtocol) {
    super(serde);
    this.pool = pool;
    this.isSetup = false;
    this.SQL_STATEMENTS = getSQLStatements();
  }

  /**
   * Creates a new instance of MysqlSaver from a connection string.
   *
   * @param {string} connString - The connection string to connect to the MySQL database.
   * @returns {MysqlSaver} A new instance of MysqlSaver.
   *
   * @example
   * const connString = "mysql://user:password@localhost:3306/db";
   * const checkpointer = MysqlSaver.fromConnString(connString);
   * await checkpointer.setup();
   */
  static fromConnString(connString: string): MysqlSaver {
    const pool = mysql.createPool(connString);
    return new MysqlSaver(pool);
  }

  /**
   * Set up the checkpoint database asynchronously.
   *
   * This method creates the necessary tables in the MySQL database if they don't
   * already exist and runs database migrations. It MUST be called directly by the user
   * the first time checkpointer is used.
   */
  async setup(): Promise<void> {
    const connection = await this.pool.getConnection();
    try {
      let version = -1;
      const MIGRATIONS = getMigrations();

      try {
        const [rows] = await connection.query<mysql.RowDataPacket[]>(
          "SELECT v FROM checkpoint_migrations ORDER BY v DESC LIMIT 1"
        );
        if (rows.length > 0) {
          version = rows[0].v;
        }
      } catch (error: unknown) {
        // Assume table doesn't exist if there's an error
        version = -1;
      }

      for (let v = version + 1; v < MIGRATIONS.length; v += 1) {
        await connection.query(MIGRATIONS[v]);
        await connection.query(
          "INSERT INTO checkpoint_migrations (v) VALUES (?)",
          [v]
        );
      }
    } finally {
      connection.release();
    }
  }

  protected async _loadCheckpoint(
    checkpoint: Omit<Checkpoint, "pending_sends" | "channel_values">,
    channelValues: [Buffer, Buffer, Buffer][]
  ): Promise<Checkpoint> {
    return {
      ...checkpoint,
      channel_values: await this._loadBlobs(channelValues),
    };
  }

  protected async _loadBlobs(
    blobValues: [Buffer, Buffer, Buffer][] | null
  ): Promise<Record<string, unknown>> {
    if (!blobValues || blobValues.length === 0) {
      return {};
    }
    const entries = await Promise.all(
      blobValues
        .filter(([, t]) => t.toString("utf-8") !== "empty")
        .map(async ([k, t, v]) => [
          k.toString("utf-8"),
          await this.serde.loadsTyped(t.toString("utf-8"), new Uint8Array(v)),
        ])
    );
    return Object.fromEntries(entries);
  }

  // eslint-disable-next-line class-methods-use-this
  protected async _loadMetadata(metadata: Record<string, unknown>) {
    const [type, dumpedValue] = await this.serde.dumpsTyped(metadata);
    return this.serde.loadsTyped(type, dumpedValue);
  }

  protected async _loadWritesFromRows(
    writes: SQL_TYPES["SELECT_PENDING_WRITES_SQL"][]
  ): Promise<[string, string, unknown][]> {
    if (!writes || writes.length === 0) {
      return [];
    }
    return Promise.all(
      writes.map(async (write) => {
        const typeStr = write.type;
        const valueArray = new Uint8Array(write.blob);
        return [
          write.task_id,
          write.channel,
          await this.serde.loadsTyped(typeStr, valueArray),
        ];
      })
    );
  }

  protected async _loadWrites(
    writes: [Buffer, Buffer, Buffer, Buffer][] | null
  ): Promise<[string, string, unknown][]> {
    if (!writes) {
      return [];
    }
    return Promise.all(
      writes.map(async ([tid, channel, t, v]) => {
        const typeStr = t.toString("utf-8");
        const valueArray = new Uint8Array(v);
        return [
          tid.toString("utf-8"),
          channel.toString("utf-8"),
          await this.serde.loadsTyped(typeStr, valueArray),
        ];
      })
    );
  }

  protected async _dumpBlobs(
    threadId: string,
    checkpointNs: string,
    values: Record<string, unknown>,
    versions: ChannelVersions
  ): Promise<[string, string, string, string, string, Buffer | undefined][]> {
    if (Object.keys(versions).length === 0) {
      return [];
    }

    return Promise.all(
      Object.entries(versions).map(async ([k, ver]) => {
        const [type, value] =
          k in values
            ? await this.serde.dumpsTyped(values[k])
            : ["empty", null];
        return [
          threadId,
          checkpointNs,
          k,
          ver.toString(),
          type,
          value ? Buffer.from(value) : undefined,
        ];
      })
    );
  }

  // eslint-disable-next-line class-methods-use-this
  protected _dumpCheckpoint(checkpoint: Checkpoint) {
    const serialized: Record<string, unknown> = { ...checkpoint };
    if ("channel_values" in serialized) delete serialized.channel_values;
    return serialized;
  }

  protected async _dumpMetadata(metadata: CheckpointMetadata) {
    const [, serializedMetadata] = await this.serde.dumpsTyped(metadata);
    // We need to remove null characters before writing
    return JSON.parse(
      new TextDecoder().decode(serializedMetadata).replace(/\0/g, "")
    );
  }

  protected async _dumpWrites(
    threadId: string,
    checkpointNs: string,
    checkpointId: string,
    taskId: string,
    writes: [string, unknown][]
  ): Promise<
    [string, string, string, string, number, string, string, Buffer][]
  > {
    return Promise.all(
      writes.map(async ([channel, value], idx) => {
        const [type, serializedValue] = await this.serde.dumpsTyped(value);
        return [
          threadId,
          checkpointNs,
          checkpointId,
          taskId,
          WRITES_IDX_MAP[channel] ?? idx,
          channel,
          type,
          Buffer.from(serializedValue),
        ];
      })
    );
  }

  /**
   * Return WHERE clause predicates for a given list() config, filter, cursor.
   *
   * This method returns a tuple of a string and a tuple of values. The string
   * is the parameterized WHERE clause predicate (including the WHERE keyword):
   * "WHERE column1 = ? AND column2 IS ?". The list of values contains the
   * values for each of the corresponding parameters.
   */
  protected _searchWhere(
    config?: RunnableConfig,
    filter?: Record<string, unknown>,
    before?: RunnableConfig
  ): [string, unknown[]] {
    const wheres: string[] = [];
    const paramValues: unknown[] = [];

    // construct predicate for config filter
    if (config?.configurable?.thread_id) {
      wheres.push("thread_id = ?");
      paramValues.push(config.configurable.thread_id);
    }

    // strict checks for undefined/null because empty strings are falsy
    if (
      config?.configurable?.checkpoint_ns !== undefined &&
      config?.configurable?.checkpoint_ns !== null
    ) {
      wheres.push("checkpoint_ns = ?");
      paramValues.push(config.configurable.checkpoint_ns);
    }

    if (config?.configurable?.checkpoint_id) {
      wheres.push("checkpoint_id = ?");
      paramValues.push(config.configurable.checkpoint_id);
    }

    // construct predicate for metadata filter
    if (filter && Object.keys(filter).length > 0) {
      wheres.push("JSON_CONTAINS(metadata, ?)");
      paramValues.push(JSON.stringify(filter));
    }

    // construct predicate for `before`
    if (before?.configurable?.checkpoint_id !== undefined) {
      wheres.push("checkpoint_id < ?");
      paramValues.push(before.configurable.checkpoint_id);
    }

    return [
      wheres.length > 0 ? `WHERE ${wheres.join(" AND ")}` : "",
      paramValues,
    ];
  }

  // eslint-disable-next-line @typescript-eslint/naming-convention, camelcase
  async getTuple(config: RunnableConfig): Promise<CheckpointTuple | undefined> {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    const {
      thread_id,
      checkpoint_ns = "",
      checkpoint_id,
    } = config.configurable ?? {};

    let args: unknown[];
    let where: string;
    // eslint-disable-next-line @typescript-eslint/naming-convention
    if (checkpoint_id) {
      where = "WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?";
      // eslint-disable-next-line @typescript-eslint/naming-convention
      args = [thread_id, checkpoint_ns, checkpoint_id];
    } else {
      where =
        "WHERE thread_id = ? AND checkpoint_ns = ? ORDER BY checkpoint_id DESC LIMIT 1";
      // eslint-disable-next-line @typescript-eslint/naming-convention
      args = [thread_id, checkpoint_ns];
    }

    const [rows] = await this.pool.query<mysql.RowDataPacket[]>(
      this.SQL_STATEMENTS.SELECT_SQL + where,
      args
    );

    const row = rows[0] as SQL_TYPES["SELECT_SQL"] | undefined;
    if (row === undefined) return undefined;

    // Fetch channel values separately
    const channelVersions = row.checkpoint.channel_versions || {};
    const channelValuesRows: [Buffer, Buffer, Buffer][] = [];
    
    if (Object.keys(channelVersions).length > 0) {
      const [cvRows] = await this.pool.query<mysql.RowDataPacket[]>(
        this.SQL_STATEMENTS.SELECT_CHANNEL_VALUES_SQL,
        [
          thread_id,
          checkpoint_ns,
          JSON.stringify(channelVersions),
          JSON.stringify(channelVersions),
        ]
      );
      
      for (const cvRow of cvRows as SQL_TYPES["SELECT_CHANNEL_VALUES_SQL"][]) {
        channelValuesRows.push([
          Buffer.from(cvRow.channel),
          Buffer.from(cvRow.type),
          cvRow.blob,
        ]);
      }
    }
    
    row.channel_values = channelValuesRows;

    // Fetch pending writes separately
    const [pendingWritesRows] = await this.pool.query<mysql.RowDataPacket[]>(
      this.SQL_STATEMENTS.SELECT_PENDING_WRITES_SQL,
      // eslint-disable-next-line @typescript-eslint/naming-convention
      [thread_id, checkpoint_ns, row.checkpoint_id]
    );
    const pendingWritesData =
      pendingWritesRows as SQL_TYPES["SELECT_PENDING_WRITES_SQL"][];

    // eslint-disable-next-line @typescript-eslint/naming-convention
    if (row.checkpoint.v < 4 && row.parent_checkpoint_id != null) {
      const [sendsRows] = await this.pool.query<mysql.RowDataPacket[]>(
        this.SQL_STATEMENTS.SELECT_PENDING_SENDS_SQL,
        // eslint-disable-next-line @typescript-eslint/naming-convention
        [thread_id, [row.parent_checkpoint_id]]
      );

      const sendsData = sendsRows as SQL_TYPES["SELECT_PENDING_SENDS_SQL"][];
      if (sendsData.length > 0) {
        const pendingSends: [Buffer, Buffer][] = sendsData.map((row) => [
          Buffer.from(row.type),
          row.blob,
        ]);
        await this._migratePendingSends(pendingSends, row);
      }
    }

    const checkpoint = await this._loadCheckpoint(
      row.checkpoint,
      row.channel_values
    );

    const finalConfig = {
      configurable: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        thread_id,
        // eslint-disable-next-line @typescript-eslint/naming-convention
        checkpoint_ns,
        // eslint-disable-next-line @typescript-eslint/naming-convention
        checkpoint_id: row.checkpoint_id,
      },
    };
    const metadata = await this._loadMetadata(row.metadata);
    // eslint-disable-next-line @typescript-eslint/naming-convention
    const parentConfig = row.parent_checkpoint_id
      ? {
          configurable: {
            // eslint-disable-next-line @typescript-eslint/naming-convention
            thread_id,
            // eslint-disable-next-line @typescript-eslint/naming-convention
            checkpoint_ns,
            // eslint-disable-next-line @typescript-eslint/naming-convention
            checkpoint_id: row.parent_checkpoint_id,
          },
        }
      : undefined;
    const pendingWrites = await this._loadWritesFromRows(pendingWritesData);

    return {
      config: finalConfig,
      checkpoint,
      metadata,
      parentConfig,
      pendingWrites,
    };
  }

  /**
   * List checkpoints from the database.
   *
   * This method retrieves a list of checkpoint tuples from the MySQL database based
   * on the provided config. The checkpoints are ordered by checkpoint ID in descending order (newest first).
   */
  async *list(
    config: RunnableConfig,
    options?: CheckpointListOptions
  ): AsyncGenerator<CheckpointTuple> {
    const { filter, before, limit } = options ?? {};
    const [where, args] = this._searchWhere(config, filter, before);
    let query = `${this.SQL_STATEMENTS.SELECT_SQL}${where} ORDER BY checkpoint_id DESC`;
    if (limit !== undefined) {
      query += ` LIMIT ${Number.parseInt(limit.toString(), 10)}`; // sanitize via parseInt, as limit could be an externally provided value
    }

    const [rows] = await this.pool.query<mysql.RowDataPacket[]>(query, args);
    const typedRows = rows as SQL_TYPES["SELECT_SQL"][];

    const toMigrate = typedRows.filter(
      (row) => row.checkpoint.v < 4 && row.parent_checkpoint_id != null
    );

    if (toMigrate.length > 0) {
      const [sendsRows] = await this.pool.query<mysql.RowDataPacket[]>(
        this.SQL_STATEMENTS.SELECT_PENDING_SENDS_SQL,
        [
          toMigrate[0].thread_id,
          toMigrate.map((row) => row.parent_checkpoint_id),
        ]
      );

      const sendsData = sendsRows as SQL_TYPES["SELECT_PENDING_SENDS_SQL"][];
      
      // Group by checkpoint_id
      const sendsByCheckpointId: Record<string, [Buffer, Buffer][]> = {};
      for (const sendRow of sendsData) {
        if (!sendsByCheckpointId[sendRow.checkpoint_id]) {
          sendsByCheckpointId[sendRow.checkpoint_id] = [];
        }
        sendsByCheckpointId[sendRow.checkpoint_id].push([
          Buffer.from(sendRow.type),
          sendRow.blob,
        ]);
      }

      const parentMap = toMigrate.reduce<
        Record<string, SQL_TYPES["SELECT_SQL"][]>
      >((acc, row) => {
        if (!row.parent_checkpoint_id) return acc;

        acc[row.parent_checkpoint_id] ??= [];
        acc[row.parent_checkpoint_id].push(row);
        return acc;
      }, {});

      // add to values
      for (const [checkpointId, pendingSends] of Object.entries(sendsByCheckpointId)) {
        for (const row of parentMap[checkpointId] || []) {
          await this._migratePendingSends(pendingSends, row);
        }
      }
    }

    for (const value of typedRows) {
      // Fetch channel values for this checkpoint
      const channelVersions = value.checkpoint.channel_versions || {};
      const channelValuesRows: [Buffer, Buffer, Buffer][] = [];
      
      if (Object.keys(channelVersions).length > 0) {
        const [cvRows] = await this.pool.query<mysql.RowDataPacket[]>(
          this.SQL_STATEMENTS.SELECT_CHANNEL_VALUES_SQL,
          [
            value.thread_id,
            value.checkpoint_ns,
            JSON.stringify(channelVersions),
            JSON.stringify(channelVersions),
          ]
        );
        
        for (const cvRow of cvRows as SQL_TYPES["SELECT_CHANNEL_VALUES_SQL"][]) {
          channelValuesRows.push([
            Buffer.from(cvRow.channel),
            Buffer.from(cvRow.type),
            cvRow.blob,
          ]);
        }
      }
      
      value.channel_values = channelValuesRows;
      
      // Fetch pending writes for this checkpoint
      const [pendingWritesRows] = await this.pool.query<mysql.RowDataPacket[]>(
        this.SQL_STATEMENTS.SELECT_PENDING_WRITES_SQL,
        [value.thread_id, value.checkpoint_ns, value.checkpoint_id]
      );
      const pendingWritesData =
        pendingWritesRows as SQL_TYPES["SELECT_PENDING_WRITES_SQL"][];

      yield {
        config: {
          configurable: {
            thread_id: value.thread_id,
            checkpoint_ns: value.checkpoint_ns,
            checkpoint_id: value.checkpoint_id,
          },
        },
        checkpoint: await this._loadCheckpoint(
          value.checkpoint,
          value.channel_values
        ),
        metadata: await this._loadMetadata(value.metadata),
        parentConfig: value.parent_checkpoint_id
          ? {
              configurable: {
                thread_id: value.thread_id,
                checkpoint_ns: value.checkpoint_ns,
                checkpoint_id: value.parent_checkpoint_id,
              },
            }
          : undefined,
        pendingWrites: await this._loadWritesFromRows(pendingWritesData),
      };
    }
  }

  /** @internal */
  async _migratePendingSends(
    pendingSends: [Buffer, Buffer][],
    mutableRow: {
      channel_values: [Buffer, Buffer, Buffer][];
      checkpoint: Omit<Checkpoint, "pending_sends" | "channel_values">;
    }
  ) {
    const row = mutableRow;

    const [enc, blob] = await this.serde.dumpsTyped(
      await Promise.all(
        pendingSends.map(([encBuf, blobBuf]) =>
          this.serde.loadsTyped(
            encBuf.toString("utf-8"),
            new Uint8Array(blobBuf)
          )
        )
      )
    );

    row.channel_values ??= [];
    row.channel_values.push([
      Buffer.from(TASKS, "utf-8"),
      Buffer.from(enc, "utf-8"),
      Buffer.from(blob),
    ]);

    // add to versions
    row.checkpoint.channel_versions[TASKS] =
      Object.keys(mutableRow.checkpoint.channel_versions).length > 0
        ? maxChannelVersion(
            ...Object.values(mutableRow.checkpoint.channel_versions)
          )
        : this.getNextVersion(undefined);
  }

  /**
   * Save a checkpoint to the database.
   *
   * This method saves a checkpoint to the MySQL database. The checkpoint is associated
   * with the provided config and its parent config (if any).
   * @param config
   * @param checkpoint
   * @param metadata
   * @returns
   */
  async put(
    config: RunnableConfig,
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata,
    newVersions: ChannelVersions
  ): Promise<RunnableConfig> {
    if (config.configurable === undefined) {
      throw new Error(`Missing "configurable" field in "config" param`);
    }
    // eslint-disable-next-line @typescript-eslint/naming-convention
    const {
      thread_id,
      checkpoint_ns = "",
      checkpoint_id,
    } = config.configurable;

    const nextConfig = {
      configurable: {
        // eslint-disable-next-line @typescript-eslint/naming-convention
        thread_id,
        // eslint-disable-next-line @typescript-eslint/naming-convention
        checkpoint_ns,
        // eslint-disable-next-line @typescript-eslint/naming-convention
        checkpoint_id: checkpoint.id,
      },
    };
    const connection = await this.pool.getConnection();
    const serializedCheckpoint = this._dumpCheckpoint(checkpoint);
    try {
      await connection.beginTransaction();
      const serializedBlobs = await this._dumpBlobs(
        // eslint-disable-next-line @typescript-eslint/naming-convention
        thread_id,
        // eslint-disable-next-line @typescript-eslint/naming-convention
        checkpoint_ns,
        checkpoint.channel_values,
        newVersions
      );
      for (const serializedBlob of serializedBlobs) {
        await connection.query(
          this.SQL_STATEMENTS.UPSERT_CHECKPOINT_BLOBS_SQL,
          serializedBlob
        );
      }
      await connection.query(this.SQL_STATEMENTS.UPSERT_CHECKPOINTS_SQL, [
        // eslint-disable-next-line @typescript-eslint/naming-convention
        thread_id,
        // eslint-disable-next-line @typescript-eslint/naming-convention
        checkpoint_ns,
        checkpoint.id,
        // eslint-disable-next-line @typescript-eslint/naming-convention
        checkpoint_id,
        JSON.stringify(serializedCheckpoint),
        JSON.stringify(await this._dumpMetadata(metadata)),
      ]);
      await connection.commit();
    } catch (e) {
      await connection.rollback();
      throw e;
    } finally {
      connection.release();
    }
    return nextConfig;
  }

  /**
   * Store intermediate writes linked to a checkpoint.
   *
   * This method saves intermediate writes associated with a checkpoint to the MySQL database.
   * @param config Configuration of the related checkpoint.
   * @param writes List of writes to store.
   * @param taskId Identifier for the task creating the writes.
   */
  async putWrites(
    config: RunnableConfig,
    writes: PendingWrite[],
    taskId: string
  ): Promise<void> {
    const query = writes.every((w) => w[0] in WRITES_IDX_MAP)
      ? this.SQL_STATEMENTS.UPSERT_CHECKPOINT_WRITES_SQL
      : this.SQL_STATEMENTS.INSERT_CHECKPOINT_WRITES_SQL;

    const dumpedWrites = await this._dumpWrites(
      config.configurable?.thread_id,
      config.configurable?.checkpoint_ns,
      config.configurable?.checkpoint_id,
      taskId,
      writes
    );
    const connection = await this.pool.getConnection();
    try {
      await connection.beginTransaction();
      for await (const dumpedWrite of dumpedWrites) {
        await connection.query(query, dumpedWrite);
      }
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  }

  async end() {
    return this.pool.end();
  }

  async deleteThread(threadId: string): Promise<void> {
    const connection = await this.pool.getConnection();
    try {
      await connection.beginTransaction();
      await connection.query(this.SQL_STATEMENTS.DELETE_CHECKPOINT_BLOBS_SQL, [
        threadId,
      ]);
      await connection.query(this.SQL_STATEMENTS.DELETE_CHECKPOINTS_SQL, [
        threadId,
      ]);
      await connection.query(this.SQL_STATEMENTS.DELETE_CHECKPOINT_WRITES_SQL, [
        threadId,
      ]);
      await connection.commit();
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  }
}
