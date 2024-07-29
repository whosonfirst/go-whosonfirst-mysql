package sql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/aaronland/go-roster"	
)

type Table interface {
	Name() string
	Schema() string
	InitializeTable(context.Context, Database) error
	IndexFeature(context.Context, *sql.Tx, []byte, ...interface{}) error
}

var table_roster roster.Roster

// TableInitializationFunc is a function defined by individual table package and used to create
// an instance of that table
type TableInitializationFunc func(ctx context.Context, uri string) (Table, error)

// RegisterTable registers 'scheme' as a key pointing to 'init_func' in an internal lookup table
// used to create new `Table` instances by the `NewTable` method.
func RegisterTable(ctx context.Context, scheme string, init_func TableInitializationFunc) error {

	err := ensureTableRoster()

	if err != nil {
		return err
	}

	return table_roster.Register(ctx, scheme, init_func)
}

func ensureTableRoster() error {

	if table_roster == nil {

		r, err := roster.NewDefaultRoster()

		if err != nil {
			return err
		}

		table_roster = r
	}

	return nil
}

// NewTable returns a new `Table` instance configured by 'uri'. The value of 'uri' is parsed
// as a `url.URL` and its scheme is used as the key for a corresponding `TableInitializationFunc`
// function used to instantiate the new `Table`. It is assumed that the scheme (and initialization
// function) have been registered by the `RegisterTable` method.
func NewTable(ctx context.Context, uri string) (Table, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, err
	}

	scheme := u.Scheme

	i, err := table_roster.Driver(ctx, scheme)

	if err != nil {
		return nil, err
	}

	init_func := i.(TableInitializationFunc)
	return init_func(ctx, uri)
}

// Schemes returns the list of schemes that have been registered.
func Schemes() []string {

	ctx := context.Background()
	schemes := []string{}

	err := ensureTableRoster()

	if err != nil {
		return schemes
	}

	for _, dr := range table_roster.Drivers(ctx) {
		scheme := fmt.Sprintf("%s://", strings.ToLower(dr))
		schemes = append(schemes, scheme)
	}

	sort.Strings(schemes)
	return schemes
}
