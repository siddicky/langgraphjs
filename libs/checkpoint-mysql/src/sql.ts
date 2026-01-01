import { type Checkpoint, TASKS } from "@langchain/langgraph-checkpoint";

export interface SQL_STATEMENTS {
  SELECT_SQL: string;
  SELECT_CHANNEL_VALUES_SQL: string;
  SELECT_PENDING_WRITES_SQL: string;
  SELECT_PENDING_SENDS_SQL: string;
  UPSERT_CHECKPOINT_BLOBS_SQL: string;
  UPSERT_CHECKPOINTS_SQL: string;
  UPSERT_CHECKPOINT_WRITES_SQL: string;
  INSERT_CHECKPOINT_WRITES_SQL: string;
  DELETE_CHECKPOINTS_SQL: string;
  DELETE_CHECKPOINT_BLOBS_SQL: string;
  DELETE_CHECKPOINT_WRITES_SQL: string;
}

export type SQL_TYPES = {
  SELECT_SQL: {
    channel_values: [Buffer, Buffer, Buffer][];
    checkpoint: Omit<Checkpoint, "pending_sends" | "channel_values">;
    parent_checkpoint_id: string | null;
    thread_id: string;
    checkpoint_ns: string;
    checkpoint_id: string;
    metadata: Record<string, unknown>;
  };
  SELECT_CHANNEL_VALUES_SQL: {
    channel: string;
    type: string;
    blob: Buffer;
  };
  SELECT_PENDING_WRITES_SQL: {
    task_id: string;
    channel: string;
    type: string;
    blob: Buffer;
  };
  SELECT_PENDING_SENDS_SQL: {
    checkpoint_id: string;
    type: string;
    blob: Buffer;
  };
  UPSERT_CHECKPOINT_BLOBS_SQL: unknown;
  UPSERT_CHECKPOINTS_SQL: unknown;
  UPSERT_CHECKPOINT_WRITES_SQL: unknown;
  INSERT_CHECKPOINT_WRITES_SQL: unknown;
  DELETE_CHECKPOINTS_SQL: unknown;
  DELETE_CHECKPOINT_BLOBS_SQL: unknown;
  DELETE_CHECKPOINT_WRITES_SQL: unknown;
};

interface TABLES {
  checkpoints: string;
  checkpoint_blobs: string;
  checkpoint_writes: string;
  checkpoint_migrations: string;
}

export const getTables = (): TABLES => ({
  checkpoints: "checkpoints",
  checkpoint_blobs: "checkpoint_blobs",
  checkpoint_migrations: "checkpoint_migrations",
  checkpoint_writes: "checkpoint_writes",
});

export const getSQLStatements = (): SQL_STATEMENTS => {
  const TABLES = getTables();
  return {
    SELECT_SQL: `SELECT
    cp.thread_id,
    cp.checkpoint,
    cp.checkpoint_ns,
    cp.checkpoint_id,
    cp.parent_checkpoint_id,
    cp.metadata
  FROM ${TABLES.checkpoints} cp `,

    SELECT_CHANNEL_VALUES_SQL: `SELECT channel, type, \`blob\`
      FROM ${TABLES.checkpoint_blobs}
      WHERE thread_id = ? AND checkpoint_ns = ?
        AND JSON_CONTAINS(JSON_KEYS(?), JSON_QUOTE(channel))
        AND version = JSON_UNQUOTE(JSON_EXTRACT(?, CONCAT('$.', channel)))`,

    SELECT_PENDING_WRITES_SQL: `SELECT task_id, channel, type, \`blob\`
      FROM ${TABLES.checkpoint_writes}
      WHERE thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?
      ORDER BY task_id, idx`,

    SELECT_PENDING_SENDS_SQL: `SELECT
      checkpoint_id,
      type,
      \`blob\`
    FROM ${TABLES.checkpoint_writes}
    WHERE thread_id = ?
      AND checkpoint_id IN (?)
      AND channel = '${TASKS}'
    ORDER BY checkpoint_id, task_id, idx
  `,

    UPSERT_CHECKPOINT_BLOBS_SQL: `INSERT INTO ${TABLES.checkpoint_blobs} (thread_id, checkpoint_ns, channel, version, type, \`blob\`)
  VALUES (?, ?, ?, ?, ?, ?)
  ON DUPLICATE KEY UPDATE type = type
  `,

    UPSERT_CHECKPOINTS_SQL: `INSERT INTO ${TABLES.checkpoints} (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, checkpoint, metadata)
  VALUES (?, ?, ?, ?, ?, ?)
  ON DUPLICATE KEY UPDATE
    checkpoint = VALUES(checkpoint),
    metadata = VALUES(metadata)
  `,

    UPSERT_CHECKPOINT_WRITES_SQL: `INSERT INTO ${TABLES.checkpoint_writes} (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, \`blob\`)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  ON DUPLICATE KEY UPDATE
    channel = VALUES(channel),
    type = VALUES(type),
    \`blob\` = VALUES(\`blob\`)
  `,

    INSERT_CHECKPOINT_WRITES_SQL: `INSERT IGNORE INTO ${TABLES.checkpoint_writes} (thread_id, checkpoint_ns, checkpoint_id, task_id, idx, channel, type, \`blob\`)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `,

    DELETE_CHECKPOINTS_SQL: `DELETE FROM ${TABLES.checkpoints} WHERE thread_id = ?`,
    DELETE_CHECKPOINT_BLOBS_SQL: `DELETE FROM ${TABLES.checkpoint_blobs} WHERE thread_id = ?`,
    DELETE_CHECKPOINT_WRITES_SQL: `DELETE FROM ${TABLES.checkpoint_writes} WHERE thread_id = ?`,
  };
};
