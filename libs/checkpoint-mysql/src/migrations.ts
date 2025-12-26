import { getTables } from "./sql.js";

/**
 * To add a new migration, add a new string to the list returned by the getMigrations function.
 * The position of the migration in the list is the version number.
 */
export const getMigrations = () => {
  const TABLES = getTables();
  return [
    `CREATE TABLE IF NOT EXISTS ${TABLES.checkpoint_migrations} (
    v INT PRIMARY KEY
  )`,
    `CREATE TABLE IF NOT EXISTS ${TABLES.checkpoints} (
    thread_id VARCHAR(191) NOT NULL,
    checkpoint_ns VARCHAR(191) NOT NULL DEFAULT '',
    checkpoint_id VARCHAR(191) NOT NULL,
    parent_checkpoint_id VARCHAR(191),
    type VARCHAR(191),
    checkpoint JSON NOT NULL,
    metadata JSON NOT NULL,
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
  )`,
    `CREATE TABLE IF NOT EXISTS ${TABLES.checkpoint_blobs} (
    thread_id VARCHAR(191) NOT NULL,
    checkpoint_ns VARCHAR(191) NOT NULL DEFAULT '',
    channel VARCHAR(191) NOT NULL,
    version VARCHAR(191) NOT NULL,
    type VARCHAR(191) NOT NULL,
    \`blob\` LONGBLOB,
    PRIMARY KEY (thread_id, checkpoint_ns, channel, version)
  )`,
    `CREATE TABLE IF NOT EXISTS ${TABLES.checkpoint_writes} (
    thread_id VARCHAR(191) NOT NULL,
    checkpoint_ns VARCHAR(191) NOT NULL DEFAULT '',
    checkpoint_id VARCHAR(191) NOT NULL,
    task_id VARCHAR(191) NOT NULL,
    idx INT NOT NULL,
    channel VARCHAR(191) NOT NULL,
    type VARCHAR(191),
    \`blob\` LONGBLOB NOT NULL,
    PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
  )`,
    `ALTER TABLE ${TABLES.checkpoint_blobs} MODIFY COLUMN \`blob\` LONGBLOB NULL`,
  ];
};
