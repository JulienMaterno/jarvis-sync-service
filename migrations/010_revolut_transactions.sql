-- =============================================================================
-- Revolut Transactions Table
-- =============================================================================
-- Schema based on Revolut CSV export format.
-- Stores all financial transactions for expense tracking and analysis.
-- =============================================================================

-- Drop any legacy transaction tables if they exist
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS expenses CASCADE;
DROP TABLE IF EXISTS financial_transactions CASCADE;

-- =============================================================================
-- REVOLUT TRANSACTIONS TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS revolut_transactions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Transaction Type & Product
    type TEXT NOT NULL,                    -- 'Card Payment', 'Exchange', 'Topup', 'Transfer', 'Charge'
    product TEXT,                          -- 'Current', 'Savings'
    
    -- Dates
    started_date TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_date TIMESTAMP WITH TIME ZONE,
    
    -- Transaction Details
    description TEXT,                      -- Merchant name, transfer details, etc.
    amount DECIMAL(12, 2) NOT NULL,        -- Positive for income, negative for expenses
    fee DECIMAL(12, 2) DEFAULT 0.00,       -- Transaction fees
    currency TEXT NOT NULL,                -- 'EUR', 'USD', 'AUD', 'SGD', etc.
    
    -- Status & Balance
    state TEXT,                            -- 'COMPLETED', 'PENDING', 'REVERTED', etc.
    balance DECIMAL(12, 2),                -- Account balance after transaction
    
    -- Categorization (can be auto-filled by AI later)
    category TEXT,                         -- 'Food', 'Travel', 'Shopping', 'Income', etc.
    subcategory TEXT,                      -- More specific category
    tags TEXT[],                           -- Custom tags
    notes TEXT,                            -- User notes
    
    -- Deduplication
    revolut_hash TEXT UNIQUE,              -- Hash of type+date+amount+description for dedup
    
    -- Contact linking (for transfers to known people)
    contact_id UUID REFERENCES contacts(id) ON DELETE SET NULL,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- =============================================================================
-- INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_revolut_transactions_started_date 
    ON revolut_transactions(started_date DESC);
    
CREATE INDEX IF NOT EXISTS idx_revolut_transactions_type 
    ON revolut_transactions(type);
    
CREATE INDEX IF NOT EXISTS idx_revolut_transactions_currency 
    ON revolut_transactions(currency);
    
CREATE INDEX IF NOT EXISTS idx_revolut_transactions_category 
    ON revolut_transactions(category);
    
CREATE INDEX IF NOT EXISTS idx_revolut_transactions_product 
    ON revolut_transactions(product);

CREATE INDEX IF NOT EXISTS idx_revolut_transactions_amount 
    ON revolut_transactions(amount);

-- =============================================================================
-- USEFUL VIEWS
-- =============================================================================

-- Monthly spending summary by category
CREATE OR REPLACE VIEW monthly_spending_summary AS
SELECT 
    date_trunc('month', started_date) AS month,
    currency,
    category,
    COUNT(*) AS transaction_count,
    SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) AS total_spent,
    SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS total_income,
    AVG(CASE WHEN amount < 0 THEN ABS(amount) ELSE NULL END) AS avg_expense
FROM revolut_transactions
WHERE state = 'COMPLETED'
GROUP BY date_trunc('month', started_date), currency, category
ORDER BY month DESC, total_spent DESC;

-- Daily spending for recent transactions
CREATE OR REPLACE VIEW daily_spending AS
SELECT 
    DATE(started_date) AS date,
    currency,
    COUNT(*) AS transaction_count,
    SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) AS spent,
    SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS received
FROM revolut_transactions
WHERE state = 'COMPLETED'
  AND started_date >= NOW() - INTERVAL '30 days'
GROUP BY DATE(started_date), currency
ORDER BY date DESC;

-- =============================================================================
-- TRIGGER FOR updated_at
-- =============================================================================

CREATE OR REPLACE FUNCTION update_revolut_transactions_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = timezone('utc'::text, now());
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_revolut_transactions_updated_at ON revolut_transactions;
CREATE TRIGGER trigger_update_revolut_transactions_updated_at
    BEFORE UPDATE ON revolut_transactions
    FOR EACH ROW
    EXECUTE FUNCTION update_revolut_transactions_updated_at();

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON TABLE revolut_transactions IS 'Financial transactions imported from Revolut account statements';
COMMENT ON COLUMN revolut_transactions.revolut_hash IS 'MD5 hash for deduplication during import';
COMMENT ON COLUMN revolut_transactions.category IS 'AI-assigned or manual expense category';
