package postgresql

import (
	"context"
	"github.com/jackc/pgx/v4"
	"go.uber.org/zap"
	"strconv"
	"strings"
)

// listParameters defines fields that affect SELECT SQL query in List method
type listParameters struct {
	merchantID int64
	offerID    int64
	nameQuery  string
}

const (
	// merchant_id column in databased defined to be grater than zero
	defaultMerchantID = 0
	// offer_id column in databased defined to be grater than zero
	defaultOfferID = 0
	// name column in database defined not to be blank
	defaultNameQuery = ""
)

// isAnyNonDefault returns true only if all fields in listParameters equal to default values
func (lp listParameters) isAnyNonDefault() bool {
	return lp.merchantID == defaultMerchantID || lp.offerID == defaultOfferID || lp.nameQuery == defaultNameQuery
}

// ListOption type represents function to modify listParameters struct
type ListOption func(parameters *listParameters)

// WithMerchantID applies passed id as merchantID in listParameters struct
func WithMerchantID(id int64) ListOption {
	return func(p *listParameters) {
		p.merchantID = id
	}
}

// WithOfferID applies passed id as offerID in listParameters struct
func WithOfferID(id int64) ListOption {
	return func(p *listParameters) {
		p.offerID = id
	}
}

// WithNameQuery applies passed query as nameQuery in listParameters struct
func WithNameQuery(q string) ListOption {
	return func(p *listParameters) {
		p.nameQuery = q
	}
}

// List returns Product slice from database applying ListOptions if presented.
func (s *Storage) List(ctx context.Context, options ...ListOption) ([]Product, error) {
	parameters := &listParameters{
		merchantID: defaultMerchantID,
		offerID:    defaultOfferID,
		nameQuery:  defaultNameQuery,
	}

	for _, opt := range options {
		opt(parameters)
	}

	var rows pgx.Rows
	var err error

	b := strings.Builder{}
	b.WriteString("SELECT * FROM products")

	if parameters.isAnyNonDefault() {
		b.WriteString(" WHERE 1 = 1")

		if parameters.merchantID != defaultMerchantID {
			b.WriteString(" AND merchant_id = " + strconv.FormatInt(parameters.merchantID, 10))
		}

		if parameters.offerID != defaultOfferID {
			b.WriteString(" AND offer_id = " + strconv.FormatInt(parameters.offerID, 10))
		}

		if parameters.nameQuery != defaultNameQuery {
			b.WriteString(" AND name ^@ $1")
			rows, err = s.db.Query(ctx, b.String(), parameters.nameQuery)
		} else {
			sql := b.String()
			rows, err = s.db.Query(ctx, sql)
		}
	}

	if err != nil {
		s.logger.Error("Selecting rows", zap.Error(err))
		return nil, err
	}

	var products []Product
	for rows.Next() {
		var p Product
		err = rows.Scan(&p.MerchantID, &p.OfferID, &p.Name, &p.Price, &p.Quantity)
		if err != nil {
			s.logger.Error("Scanning row", zap.Error(err))
			return nil, err
		}

		products = append(products, p)
	}

	if rows.Err() != nil {
		return nil, err
	}

	return products, nil
}
