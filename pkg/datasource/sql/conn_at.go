/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sql

import (
	"context"
	gosql "database/sql"
	"database/sql/driver"
	"errors"
	"strings"

	"seata.apache.org/seata-go/v2/pkg/datasource/sql/exec"
	"seata.apache.org/seata-go/v2/pkg/datasource/sql/types"
	"seata.apache.org/seata-go/v2/pkg/tm"
	"seata.apache.org/seata-go/v2/pkg/util/log"
)

// rowsWithStmt wraps driver.Rows and closes the statement when rows are closed
type rowsWithStmt struct {
	driver.Rows
	stmt driver.Stmt
}

func (r *rowsWithStmt) Close() error {
	rowsErr := r.Rows.Close()
	stmtErr := r.stmt.Close()
	if rowsErr != nil {
		return rowsErr
	}
	return stmtErr
}

// ATConn Database connection proxy object under XA transaction model
// Conn is assumed to be stateful.
type ATConn struct {
	*Conn
}

func (c *ATConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.createOnceTxContext(ctx) {
		defer func() {
			c.txCtx = types.NewTxCtx()
		}()
	}
	return c.Conn.PrepareContext(ctx, query)
}

// ExecContext
func (c *ATConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.createOnceTxContext(ctx) {
		defer func() {
			c.txCtx = types.NewTxCtx()
		}()
	}

	ret, err := c.createTxAndExecIfNeeded(ctx, func() (types.ExecResult, error) {
		executor, err := exec.BuildExecutor(c.res.dbType, c.txCtx.TransactionMode, query)
		if err != nil {
			return nil, err
		}

		execCtx := c.newExecContext(c.txCtx, query, nil, args)

		ret, err := executor.ExecWithNamedValue(ctx, execCtx,
			func(ctx context.Context, query string, args []driver.NamedValue) (types.ExecResult, error) {
				ret, err := c.Conn.ExecContext(ctx, query, args)
				if err == nil {
					return types.NewResult(types.WithResult(ret)), nil
				}

				// If skip fast-path error, fallback to prepared statement
				if strings.Contains(err.Error(), "skip fast-path") {
					stmt, prepErr := c.Prepare(query)
					if prepErr != nil {
						return nil, prepErr
					}
					defer stmt.Close()

					var result driver.Result
					if stmtExecCtx, ok := stmt.(driver.StmtExecContext); ok {
						result, err = stmtExecCtx.ExecContext(ctx, args)
					} else {
						dargs := make([]driver.Value, len(args))
						for i, arg := range args {
							dargs[i] = arg.Value
						}
						result, err = stmt.Exec(dargs)
					}

					if err != nil {
						return nil, err
					}

					return types.NewResult(types.WithResult(result)), nil
				}

				return nil, err
			})

		if err != nil {
			return nil, err
		}
		return ret, nil
	})
	if err != nil {
		return nil, err
	}
	return ret.GetResult(), nil
}

// QueryContext
func (c *ATConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.createOnceTxContext(ctx) {
		defer func() {
			c.txCtx = types.NewTxCtx()
		}()
	}

	ret, err := c.createTxAndQueryIfNeeded(ctx, func() (types.ExecResult, error) {
		executor, err := exec.BuildExecutor(c.res.dbType, c.txCtx.TransactionMode, query)
		if err != nil {
			return nil, err
		}

		execCtx := c.newExecContext(c.txCtx, query, nil, args)

		ret, err := executor.ExecWithNamedValue(ctx, execCtx,
			func(ctx context.Context, query string, args []driver.NamedValue) (types.ExecResult, error) {
				rows, err := c.Conn.QueryContext(ctx, query, args)
				if err == nil {
					return types.NewResult(types.WithRows(rows)), nil
				}

				// If skip fast-path error, fallback to prepared statement
				if strings.Contains(err.Error(), "skip fast-path") {
					stmt, prepErr := c.Prepare(query)
					if prepErr != nil {
						return nil, prepErr
					}

					if stmtQueryCtx, ok := stmt.(driver.StmtQueryContext); ok {
						rows, err = stmtQueryCtx.QueryContext(ctx, args)
					} else {
						dargs := make([]driver.Value, len(args))
						for i, arg := range args {
							dargs[i] = arg.Value
						}
						rows, err = stmt.Query(dargs)
					}

					if err != nil {
						stmt.Close()
						return nil, err
					}

					// Wrap rows with statement to close both together
					wrappedRows := &rowsWithStmt{Rows: rows, stmt: stmt}
					return types.NewResult(types.WithRows(wrappedRows)), nil
				}

				return nil, err
			})

		if err != nil {
			return nil, err
		}
		return ret, nil
	})
	if err != nil {
		return nil, err
	}
	return ret.GetRows(), nil
}

// BeginTx
func (c *ATConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.autoCommit = false

	// Only create new txCtx if not already in a global transaction (auto-commit case)
	if c.txCtx.XID == "" {
		c.txCtx = types.NewTxCtx()
		c.txCtx.DBType = c.res.dbType
		c.txCtx.TxOpt = opts
		c.txCtx.ResourceID = c.res.resourceID

		if tm.IsGlobalTx(ctx) {
			c.txCtx.XID = tm.GetXID(ctx)
			c.txCtx.TransactionMode = types.ATMode
		}
	}

	tx, err := c.Conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &ATTx{tx: tx.(*Tx)}, nil
}

func (c *ATConn) createOnceTxContext(ctx context.Context) bool {
	onceTx := tm.IsGlobalTx(ctx) && c.autoCommit

	if onceTx {
		c.txCtx = types.NewTxCtx()
		c.txCtx.DBType = c.res.dbType
		c.txCtx.ResourceID = c.res.resourceID
		c.txCtx.XID = tm.GetXID(ctx)
		c.txCtx.TransactionMode = types.ATMode
		c.txCtx.GlobalLockRequire = true
	}

	return onceTx
}

// createTxAndExecIfNeeded creates a transaction for execution context and commits it after execution
func (c *ATConn) createTxAndExecIfNeeded(ctx context.Context, f func() (types.ExecResult, error)) (types.ExecResult, error) {
	var (
		tx  driver.Tx
		err error
	)

	if c.txCtx.TransactionMode != types.Local && tm.IsGlobalTx(ctx) && c.autoCommit {
		tx, err = c.BeginTx(ctx, driver.TxOptions{Isolation: driver.IsolationLevel(gosql.LevelDefault)})
		if err != nil {
			return nil, err
		}
		defer func() {
			recoverErr := recover()
			if recoverErr != nil {
				log.Errorf("at exec panic, recoverErr:%v", recoverErr)
				if tx != nil {
					rollbackErr := tx.Rollback()
					if rollbackErr != nil {
						log.Errorf("conn at rollback error:%v", rollbackErr)
					}
				}
			}
		}()
	}

	ret, err := f()
	if err != nil {
		return nil, c.rollbackCreatedTx(tx, err)
	}

	// For ExecContext, commit the transaction if it was created
	if tx != nil {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
	}

	return ret, nil
}

// createTxAndQueryIfNeeded creates a transaction for query context and wraps the rows to commit on close
func (c *ATConn) createTxAndQueryIfNeeded(ctx context.Context, f func() (types.ExecResult, error)) (types.ExecResult, error) {
	var (
		tx  driver.Tx
		err error
	)

	if c.txCtx.TransactionMode != types.Local && tm.IsGlobalTx(ctx) && c.autoCommit {
		tx, err = c.BeginTx(ctx, driver.TxOptions{Isolation: driver.IsolationLevel(gosql.LevelDefault)})
		if err != nil {
			return nil, err
		}
		defer func() {
			recoverErr := recover()
			if recoverErr != nil {
				log.Errorf("at exec panic, recoverErr:%v", recoverErr)
				if tx != nil {
					rollbackErr := tx.Rollback()
					if rollbackErr != nil {
						log.Errorf("conn at rollback error:%v", rollbackErr)
					}
				}
			}
		}()
	}

	ret, err := f()
	if err != nil {
		return nil, c.rollbackCreatedTx(tx, err)
	}

	// For QueryContext, wrap rows to commit on close
	var activeTx driver.Tx
	if c.txCtx.LocalTx != nil {
		activeTx = c.txCtx.LocalTx
	} else if tx != nil {
		activeTx = tx
	}

	if activeTx != nil {
		if ret != nil {
			if dr := ret.GetRows(); dr != nil {
				wrappedRows := &RowsCommitOnClose{rows: dr, tx: activeTx}
				return types.NewResult(types.WithRows(wrappedRows)), nil
			}
		}
	}

	return ret, nil
}

func (c *ATConn) rollbackCreatedTx(tx driver.Tx, execErr error) error {
	if tx == nil || execErr == nil {
		return execErr
	}

	rollbackErr := tx.Rollback()
	if rollbackErr != nil {
		log.Errorf("conn at rollback error:%v", rollbackErr)
		return errors.Join(execErr, rollbackErr)
	}

	return execErr
}
