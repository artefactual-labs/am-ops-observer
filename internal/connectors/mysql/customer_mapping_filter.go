package mysql

import (
	"context"
	"fmt"
	"strings"
)

func (s *Store) sourceFilterClause(ctx context.Context, customerID string) (string, []any, error) {
	trimmed := strings.TrimSpace(customerID)
	if trimmed == "" || strings.EqualFold(trimmed, "all") || strings.EqualFold(trimmed, "default") {
		return "", nil, nil
	}

	if s.customerMap != nil {
		sources, err := s.customerMap.SourcesForCustomer(ctx, trimmed)
		if err != nil {
			return "", nil, err
		}
		if len(sources) == 0 {
			return "AND 1 = 0", nil, nil
		}
		placeholders := make([]string, 0, len(sources))
		args := make([]any, 0, len(sources))
		for _, src := range sources {
			placeholders = append(placeholders, "?")
			args = append(args, src)
		}
		return fmt.Sprintf("AND t.sourceOfAcquisition IN (%s)", strings.Join(placeholders, ",")), args, nil
	}

	if s.hasCustomerSourceMapping {
		return `AND EXISTS (
    SELECT 1
    FROM CustomerTransferSources cts
    WHERE cts.customer_id = ?
      AND cts.source_of_acquisition = t.sourceOfAcquisition
  )`, []any{trimmed}, nil
	}

	return "AND t.sourceOfAcquisition = ?", []any{trimmed}, nil
}
