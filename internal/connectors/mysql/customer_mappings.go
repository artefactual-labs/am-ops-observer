package mysql

import (
	"context"
	"database/sql"
	"strings"
)

// CustomerSummary represents one customer and number of mapped sources.
type CustomerSummary struct {
	CustomerID  string `json:"customer_id"`
	SourceCount int64  `json:"source_count"`
}

// CustomerMappings holds all sourceOfAcquisition values for a customer.
type CustomerMappings struct {
	CustomerID string   `json:"customer_id"`
	Sources    []string `json:"sources"`
}

// ListCustomers returns mapped customers from CustomerTransferSources.
func (s *Store) ListCustomers(ctx context.Context, limit int) ([]CustomerSummary, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	if s.customerMap != nil {
		items, err := s.customerMap.ListCustomers(ctx, limit)
		if err != nil {
			return nil, err
		}
		out := make([]CustomerSummary, 0, len(items))
		for _, item := range items {
			out = append(out, CustomerSummary{
				CustomerID:  item.CustomerID,
				SourceCount: item.SourceCount,
			})
		}
		return out, nil
	}

	const q = `
SELECT
  cts.customer_id,
  COUNT(*) AS source_count
FROM CustomerTransferSources cts
GROUP BY cts.customer_id
ORDER BY cts.customer_id ASC
LIMIT ?;
`

	rows, err := s.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]CustomerSummary, 0, limit)
	for rows.Next() {
		var item CustomerSummary
		if err := rows.Scan(&item.CustomerID, &item.SourceCount); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

// GetCustomerMappings returns all mapped source_of_acquisition values for a customer.
func (s *Store) GetCustomerMappings(ctx context.Context, customerID string) (*CustomerMappings, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	trimmed := strings.TrimSpace(customerID)
	if trimmed == "" {
		return &CustomerMappings{CustomerID: "", Sources: []string{}}, nil
	}

	if s.customerMap != nil {
		sources, err := s.customerMap.SourcesForCustomer(ctx, trimmed)
		if err != nil {
			return nil, err
		}
		return &CustomerMappings{CustomerID: trimmed, Sources: sources}, nil
	}

	const q = `
SELECT cts.source_of_acquisition
FROM CustomerTransferSources cts
WHERE cts.customer_id = ?
ORDER BY cts.source_of_acquisition ASC;
`

	rows, err := s.db.QueryContext(ctx, q, trimmed)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	sources := make([]string, 0)
	for rows.Next() {
		var source sql.NullString
		if err := rows.Scan(&source); err != nil {
			return nil, err
		}
		if source.Valid && strings.TrimSpace(source.String) != "" {
			sources = append(sources, source.String)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return &CustomerMappings{CustomerID: trimmed, Sources: sources}, nil
}

// CreateCustomerMapping inserts a single customer/source mapping if not present.
func (s *Store) CreateCustomerMapping(ctx context.Context, customerID, source string) error {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	customerID = strings.TrimSpace(customerID)
	source = strings.TrimSpace(source)

	if s.customerMap != nil {
		return s.customerMap.CreateMapping(ctx, customerID, source)
	}

	const q = `
INSERT INTO CustomerTransferSources (customer_id, source_of_acquisition)
VALUES (?, ?)
ON DUPLICATE KEY UPDATE source_of_acquisition = VALUES(source_of_acquisition);
`
	_, err := s.db.ExecContext(ctx, q, customerID, source)
	return err
}

// ReplaceCustomerMappings replaces all sources for a customer in one transaction.
func (s *Store) ReplaceCustomerMappings(ctx context.Context, customerID string, sources []string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	customerID = strings.TrimSpace(customerID)
	norm := normalizeSources(sources)

	if s.customerMap != nil {
		return s.customerMap.ReplaceMappings(ctx, customerID, norm)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `DELETE FROM CustomerTransferSources WHERE customer_id = ?`, customerID); err != nil {
		return 0, err
	}

	if len(norm) > 0 {
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO CustomerTransferSources (customer_id, source_of_acquisition) VALUES (?, ?)`)
		if err != nil {
			return 0, err
		}
		defer stmt.Close()

		for _, src := range norm {
			if _, err := stmt.ExecContext(ctx, customerID, src); err != nil {
				return 0, err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return len(norm), nil
}

// DeleteCustomerMapping deletes one source mapping for a customer.
func (s *Store) DeleteCustomerMapping(ctx context.Context, customerID, source string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	customerID = strings.TrimSpace(customerID)
	source = strings.TrimSpace(source)

	if s.customerMap != nil {
		return s.customerMap.DeleteMapping(ctx, customerID, source)
	}

	res, err := s.db.ExecContext(ctx,
		`DELETE FROM CustomerTransferSources WHERE customer_id = ? AND source_of_acquisition = ?`,
		customerID,
		source,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// DeleteAllCustomerMappings deletes all mappings for one customer.
func (s *Store) DeleteAllCustomerMappings(ctx context.Context, customerID string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, s.queryTimeout)
	defer cancel()

	customerID = strings.TrimSpace(customerID)
	if s.customerMap != nil {
		return s.customerMap.DeleteAllMappings(ctx, customerID)
	}
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM CustomerTransferSources WHERE customer_id = ?`,
		customerID,
	)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func normalizeSources(sources []string) []string {
	seen := make(map[string]struct{}, len(sources))
	out := make([]string, 0, len(sources))
	for _, src := range sources {
		src = strings.TrimSpace(src)
		if src == "" {
			continue
		}
		if _, ok := seen[src]; ok {
			continue
		}
		seen[src] = struct{}{}
		out = append(out, src)
	}
	return out
}
