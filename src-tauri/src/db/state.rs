use std::future::Future;

use sqlx::SqlitePool;
use tokio::sync::Mutex;

use crate::core::{AppError, Result};

/// Result of replacing the active SQLite pool when a failed swap can reopen the original database.
pub enum PoolReplacement {
    Replaced(SqlitePool),
    RolledBack {
        pool: Option<SqlitePool>,
        error: AppError,
    },
}

/// Holds the current SQLite pool and serializes hot replacement during backup overwrite import.
pub struct DatabaseState {
    pool: Mutex<SqlitePool>,
}

impl DatabaseState {
    /// Creates database state from the startup pool.
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool: Mutex::new(pool),
        }
    }

    /// Returns a clone of the currently active pool.
    pub async fn pool(&self) -> SqlitePool {
        self.pool.lock().await.clone()
    }

    /// Closes the active pool, runs replacement work, and installs the new pool atomically.
    pub async fn close_and_replace<F, Fut>(&self, replace: F) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<SqlitePool>>,
    {
        let mut current = self.pool.lock().await;
        current.close().await;
        let next = replace().await?;
        *current = next;
        Ok(())
    }

    /// Closes the active pool, runs replacement work, and preserves a restored pool when rolled back.
    pub async fn replace_with_rollback<F, Fut>(&self, replace: F) -> Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = PoolReplacement>,
    {
        let mut current = self.pool.lock().await;
        current.close().await;
        match replace().await {
            PoolReplacement::Replaced(next) => {
                *current = next;
                Ok(())
            }
            PoolReplacement::RolledBack { pool, error } => {
                if let Some(restored) = pool {
                    *current = restored;
                }
                Err(error)
            }
        }
    }
}
