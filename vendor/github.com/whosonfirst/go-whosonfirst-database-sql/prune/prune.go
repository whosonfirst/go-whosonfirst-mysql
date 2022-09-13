package prune

import (
	"context"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-database-sql"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/iterator"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"io"
)

// PruneTables will remove all the records in 'to_prune'
func PruneTables(ctx context.Context, db sql.Database, to_prune ...sql.Table) error {

	conn, err := db.Conn()

	if err != nil {
		return fmt.Errorf("Failed to create DB conn, because %w", err)
	}

	tx, err := conn.Begin()

	if err != nil {
		return fmt.Errorf("Failed create transaction, because %w", err)
	}

	for _, t := range to_prune {

		sql := fmt.Sprintf("DELETE FROM %s", t.Name())
		stmt, err := tx.Prepare(sql)

		if err != nil {
			return fmt.Errorf("Failed to prepare statement (%s), because %w", sql, err)
		}

		_, err = stmt.Exec()

		if err != nil {
			return fmt.Errorf("Failed to execute statement (%s), because %w", sql, err)
		}
	}

	err = tx.Commit()

	if err != nil {
		return fmt.Errorf("Failed to commit transaction, because %w", err)
	}

	return nil
}

// PruneTablesWithIterator will remove records emitted by an iterator (defined by 'iterator_uri' and 'iterator_source') from 'to_prune'.
func PruneTablesWithIterator(ctx context.Context, iterator_uri string, iterator_source string, db sql.Database, to_prune ...sql.Table) error {

	conn, err := db.Conn()

	if err != nil {
		return fmt.Errorf("Failed to create DB conn, because %w", err)
	}

	iter_cb := func(ctx context.Context, path string, r io.ReadSeeker, args ...interface{}) error {

		id, _, err := uri.ParseURI(path)

		if err != nil {
			return fmt.Errorf("Failed to parse %s, %w", path, err)
		}

		tx, err := conn.Begin()

		if err != nil {
			return fmt.Errorf("Failed create transaction for pruning %d, because %w", id, err)
		}

		for _, t := range to_prune {

			sql := fmt.Sprintf("DELETE FROM %s WHERE id = ?", t.Name())
			stmt, err := tx.Prepare(sql)

			if err != nil {
				return fmt.Errorf("Failed to prepare statement (%s), because %w", sql, err)
			}

			_, err = stmt.Exec(id)

			if err != nil {
				return fmt.Errorf("Failed to execute statement (%s, %d), because %w", sql, id, err)
			}
		}

		err = tx.Commit()

		if err != nil {
			fmt.Errorf("Failed to commit transaction to pruning %d, because %w", id, err)
		}

		return nil
	}

	iter, err := iterator.NewIterator(ctx, iterator_uri, iter_cb)

	if err != nil {
		return fmt.Errorf("Failed to create iterator, %v", err)
	}

	err = iter.IterateURIs(ctx, iterator_source)

	if err != nil {
		return fmt.Errorf("Failed to iterate URIs, %v", err)
	}

	return nil
}
