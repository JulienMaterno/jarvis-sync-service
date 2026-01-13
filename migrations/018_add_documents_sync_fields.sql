-- Migration: Add Notion sync fields to documents table
-- Purpose: Enable bidirectional sync between Notion Documents DB and Supabase
-- Run: Execute in Supabase SQL Editor

-- Add Notion sync fields
ALTER TABLE documents
ADD COLUMN IF NOT EXISTS notion_page_id TEXT UNIQUE;

ALTER TABLE documents
ADD COLUMN IF NOT EXISTS notion_updated_at TIMESTAMPTZ;

ALTER TABLE documents
ADD COLUMN IF NOT EXISTS last_sync_source TEXT;

-- Add deleted_at for soft deletes (matching other sync tables)
ALTER TABLE documents
ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

-- Add index for sync queries
CREATE INDEX IF NOT EXISTS idx_documents_notion_page_id ON documents(notion_page_id);
CREATE INDEX IF NOT EXISTS idx_documents_last_sync_source ON documents(last_sync_source);
CREATE INDEX IF NOT EXISTS idx_documents_updated_at ON documents(updated_at);

-- Comments
COMMENT ON COLUMN documents.notion_page_id IS 'Notion page ID for bidirectional sync';
COMMENT ON COLUMN documents.notion_updated_at IS 'Last edit time from Notion (for change detection)';
COMMENT ON COLUMN documents.last_sync_source IS 'Which system made the last change: notion or supabase';
COMMENT ON COLUMN documents.deleted_at IS 'Soft delete timestamp for sync safety';
