import { type Checkpoint, TASKS } from "@langchain/langgraph-checkpoint";

export interface SQL_STATEMENTS {
  SELECT_SQL: string;
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
    pending_writes: [Buffer, Buffer, Buffer, Buffer][];
  };
  SELECT_PENDING_SENDS_SQL: {
    checkpoint_id: string;
    pending_sends: [Buffer, Buffer][];
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
    cp.metadata,
    (
      SELECT JSON_ARRAYAGG(
        JSON_ARRAY(bl.channel, bl.type, bl.\`blob\`)
      )
      FROM JSON_TABLE(
        cp.checkpoint,
        '$.channel_versions' COLUMNS (
          channel VARCHAR(255) PATH '$.channel',
          version VARCHAR(255) PATH '$.version'
        )
      ) AS jt
      INNER JOIN ${TABLES.checkpoint_blobs} bl
        ON bl.thread_id = cp.thread_id
        AND bl.checkpoint_ns = cp.checkpoint_ns
        AND bl.channel COLLATE utf8mb4_unicode_ci = jt.channel
        AND bl.version COLLATE utf8mb4_unicode_ci = jt.version
    ) as channel_values,
    (
      SELECT JSON_ARRAYAGG(
        JSON_ARRAY(cw.task_id, cw.channel, cw.type, cw.\`blob\`)
      )
      FROM (
        SELECT task_id, channel, type, \`blob\`
        FROM ${TABLES.checkpoint_writes} cw2
        WHERE cw2.thread_id = cp.thread_id
          AND cw2.checkpoint_ns = cp.checkpoint_ns
          AND cw2.checkpoint_id = cp.checkpoint_id
        ORDER BY cw2.task_id, cw2.idx
      ) AS cw
    ) as pending_writes
  FROM ${TABLES.checkpoints} cp `,

    SELECT_PENDING_SENDS_SQL: `SELECT
      checkpoint_id,
      JSON_ARRAYAGG(
        JSON_ARRAY(cw.type, cw.\`blob\`)
      ) as pending_sends
    FROM (
      SELECT checkpoint_id, type, \`blob\`
      FROM ${TABLES.checkpoint_writes} cw2
      WHERE cw2.thread_id = ?
        AND cw2.checkpoint_id IN (?)
        AND cw2.channel = '${TASKS}'
      ORDER BY cw2.task_id, cw2.idx
    ) AS cw
    GROUP BY cw.checkpoint_id
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
