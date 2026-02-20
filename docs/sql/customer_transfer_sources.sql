-- Customer mapping table for monthly reporting filters.
-- A report request with customer_id=<id> will match transfers where:
--   Transfers.sourceOfAcquisition = CustomerTransferSources.source_of_acquisition
-- and CustomerTransferSources.customer_id = <id>

CREATE TABLE IF NOT EXISTS CustomerTransferSources (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  customer_id VARCHAR(255) NOT NULL,
  source_of_acquisition TEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  INDEX idx_customer_id (customer_id),
  INDEX idx_source_of_acquisition (source_of_acquisition(255)),
  UNIQUE KEY uq_customer_source (customer_id, source_of_acquisition(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
