-- Migration: Fix duplicate credit consumption on page refresh
-- Created: 2026-01-21
-- Purpose: Add deduplication check to prevent charging twice for the same video
--          when user refreshes page during AI generation

-- =====================================================
-- Add index for efficient lookup of existing generations
-- =====================================================
CREATE INDEX IF NOT EXISTS idx_video_generations_user_youtube_period
  ON video_generations (user_id, youtube_id, created_at)
  WHERE counted_toward_limit = true;

-- =====================================================
-- Function: consume_video_credit_atomically (updated)
-- Purpose: Atomically check limits and consume credits with deduplication
-- Returns: JSON with { allowed, reason, generation_id, used_topup, deduplicated }
-- =====================================================
CREATE OR REPLACE FUNCTION consume_video_credit_atomically(
  p_user_id uuid,
  p_youtube_id text,
  p_identifier text,
  p_subscription_tier text,
  p_base_limit integer,
  p_period_start timestamptz,
  p_period_end timestamptz,
  p_video_id uuid DEFAULT NULL,
  p_counted boolean DEFAULT true
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
  v_counted_usage integer;
  v_topup_credits integer;
  v_base_remaining integer;
  v_total_remaining integer;
  v_generation_id uuid;
  v_used_topup boolean := false;
  v_existing_generation_id uuid;
BEGIN
  -- Lock the user's profile row for update to prevent concurrent modifications
  SELECT topup_credits
  INTO v_topup_credits
  FROM profiles
  WHERE id = p_user_id
  FOR UPDATE;

  IF NOT FOUND THEN
    RETURN jsonb_build_object(
      'allowed', false,
      'reason', 'NO_SUBSCRIPTION',
      'error', 'Profile not found'
    );
  END IF;

  -- NEW: Check for existing generation record for this video in this period
  -- This prevents double-charging when user refreshes during AI generation
  IF p_counted THEN
    SELECT id INTO v_existing_generation_id
    FROM video_generations
    WHERE user_id = p_user_id
      AND youtube_id = p_youtube_id
      AND created_at >= p_period_start
      AND created_at <= p_period_end
      AND counted_toward_limit = true
    LIMIT 1;

    IF v_existing_generation_id IS NOT NULL THEN
      -- Already charged for this video in this period - return success without charging again
      -- Recalculate current remaining credits for accurate response
      SELECT COUNT(*)
      INTO v_counted_usage
      FROM video_generations
      WHERE user_id = p_user_id
        AND created_at >= p_period_start
        AND created_at <= p_period_end
        AND counted_toward_limit = true;

      v_base_remaining := GREATEST(0, p_base_limit - v_counted_usage);
      v_total_remaining := v_base_remaining + v_topup_credits;

      RETURN jsonb_build_object(
        'allowed', true,
        'reason', 'ALREADY_COUNTED',
        'generation_id', v_existing_generation_id,
        'used_topup', false,
        'deduplicated', true,
        'base_remaining', v_base_remaining,
        'topup_remaining', v_topup_credits,
        'total_remaining', v_total_remaining
      );
    END IF;
  END IF;

  -- Count usage in the current period (excluding cached videos)
  SELECT COUNT(*)
  INTO v_counted_usage
  FROM video_generations
  WHERE user_id = p_user_id
    AND created_at >= p_period_start
    AND created_at <= p_period_end
    AND counted_toward_limit = true;

  -- Calculate remaining credits
  v_base_remaining := GREATEST(0, p_base_limit - v_counted_usage);
  v_total_remaining := v_base_remaining + v_topup_credits;

  -- Check if user has reached limit
  IF p_counted AND v_total_remaining <= 0 THEN
    RETURN jsonb_build_object(
      'allowed', false,
      'reason', 'LIMIT_REACHED',
      'base_remaining', v_base_remaining,
      'topup_remaining', v_topup_credits,
      'total_remaining', v_total_remaining
    );
  END IF;

  -- Insert video generation record
  INSERT INTO video_generations (
    user_id,
    identifier,
    youtube_id,
    video_id,
    counted_toward_limit,
    subscription_tier
  ) VALUES (
    p_user_id,
    p_identifier,
    p_youtube_id,
    p_video_id,
    p_counted,
    p_subscription_tier
  )
  RETURNING id INTO v_generation_id;

  -- If this generation counts toward limit, consume credit
  IF p_counted THEN
    -- If base credits exhausted, consume top-up credit
    IF v_base_remaining <= 0 AND v_topup_credits > 0 THEN
      UPDATE profiles
      SET topup_credits = topup_credits - 1
      WHERE id = p_user_id
        AND topup_credits > 0;

      IF FOUND THEN
        v_used_topup := true;
        v_topup_credits := v_topup_credits - 1;
      END IF;
    END IF;
  END IF;

  -- Return success with updated values
  RETURN jsonb_build_object(
    'allowed', true,
    'reason', 'OK',
    'generation_id', v_generation_id,
    'used_topup', v_used_topup,
    'deduplicated', false,
    'base_remaining', GREATEST(0, v_base_remaining - (CASE WHEN p_counted AND NOT v_used_topup THEN 1 ELSE 0 END)),
    'topup_remaining', v_topup_credits,
    'total_remaining', v_total_remaining - (CASE WHEN p_counted THEN 1 ELSE 0 END)
  );
END;
$$;

COMMENT ON FUNCTION consume_video_credit_atomically IS
  'Atomically checks credit availability and consumes credit in single transaction. Prevents race conditions by locking profile row during check-and-consume operation. Includes deduplication to prevent double-charging when user refreshes during AI generation.';
