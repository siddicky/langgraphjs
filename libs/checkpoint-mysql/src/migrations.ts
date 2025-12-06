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
    thread_id TEXT NOT NULL,
    checkpoint_ns TEXT NOT NULL DEFAULT '',
    checkpoint_id TEXT NOT NULL,
    parent_checkpoint_id TEXT,
    type TEXT,
    checkpoint JSON NOT NULL,
    metadata JSON NOT NULL DEFAULT '{}',
    PRIMARY KEY (thread_id(255), checkpoint_ns(255), checkpoint_id(255))
  )`,
    `CREATE TABLE IF NOT EXISTS ${TABLES.checkpoint_blobs} (
    thread_id TEXT NOT NULL,
    checkpoint_ns TEXT NOT NULL DEFAULT '',
    channel TEXT NOT NULL,
    version TEXT NOT NULL,
    type TEXT NOT NULL,
    blob LONGBLOB,
    PRIMARY KEY (thread_id(255), checkpoint_ns(255), channel(255), version(255))
  )`,
    `CREATE TABLE IF NOT EXISTS ${TABLES.checkpoint_writes} (
    thread_id TEXT NOT NULL,
    checkpoint_ns TEXT NOT NULL DEFAULT '',
    checkpoint_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    idx INT NOT NULL,
    channel TEXT NOT NULL,
    type TEXT,
    blob LONGBLOB NOT NULL,
    PRIMARY KEY (thread_id(255), checkpoint_ns(255), checkpoint_id(255), task_id(255), idx)
  )`,
    `ALTER TABLE ${TABLES.checkpoint_blobs} MODIFY COLUMN blob LONGBLOB NULL`,
  ];
};
