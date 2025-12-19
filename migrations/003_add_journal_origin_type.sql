-- Migration: Add 'journal' to tasks origin_type constraint
-- Run this in Supabase SQL Editor

-- Drop the existing constraint
ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_origin_type_check;

-- Add new constraint with 'journal' included
ALTER TABLE tasks ADD CONSTRAINT tasks_origin_type_check 
  CHECK (origin_type IN ('meeting', 'reflection', 'journal', 'voice'));

-- Verify the constraint
SELECT conname, pg_get_constraintdef(oid) 
FROM pg_constraint 
WHERE conrelid = 'tasks'::regclass AND contype = 'c';
