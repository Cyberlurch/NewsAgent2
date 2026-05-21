from pathlib import Path


def test_workflow_has_managed_transcript_provider_override_and_env_wiring():
    text = Path('.github/workflows/newsagent.yml').read_text(encoding='utf-8')
    assert 'managed_transcript_provider:' in text
    assert "- transcriptapi" in text
    assert "- supadata" in text
    assert "- generic" in text
    assert "YOUTUBE_TRANSCRIPT_PROVIDER: ${{ env.MANAGED_TRANSCRIPT_PROVIDER_OVERRIDE != 'default' && env.MANAGED_TRANSCRIPT_PROVIDER_OVERRIDE || (vars.YOUTUBE_TRANSCRIPT_PROVIDER || 'none') }}" in text
    assert "YOUTUBE_TRANSCRIPT_API_KEY: ${{ secrets.YOUTUBE_TRANSCRIPT_API_KEY || '' }}" in text
    assert "YOUTUBE_TRANSCRIPT_API_BASE_URL: ${{ vars.YOUTUBE_TRANSCRIPT_API_BASE_URL || '' }}" in text
    assert "YOUTUBE_API_KEY: ${{ secrets.YOUTUBE_API_KEY || '' }}" in text
    assert "YOUTUBE_API_METADATA: ${{ vars.YOUTUBE_API_METADATA || '1' }}" in text
    assert "MANAGED_TRANSCRIPT_MAX_VIDEOS_PER_RUN: ${{ vars.MANAGED_TRANSCRIPT_MAX_VIDEOS_PER_RUN || '25' }}" in text
    assert "OPENAI_MODEL_CYBERLURCH_DIRECT_DIGEST: ${{ vars.OPENAI_MODEL_CYBERLURCH_DIRECT_DIGEST || vars.OPENAI_MODEL_CYBERLURCH_CHUNKS || vars.OPENAI_MODEL || 'gpt-4.1' }}" in text
    assert "OPENAI_MODEL_CYBERLURCH_DIRECT_DIGEST_FALLBACK: ${{ vars.OPENAI_MODEL_CYBERLURCH_DIRECT_DIGEST_FALLBACK || vars.OPENAI_MODEL_CYBERLURCH_OVERVIEW || vars.OPENAI_MODEL || 'gpt-4.1' }}" in text
    assert "CYBERLURCH_DETAIL_ITEMS_PER_DAY: ${{ vars.CYBERLURCH_DETAIL_ITEMS_PER_DAY || '10' }}" in text
    assert "CYBERLURCH_DETAIL_ITEMS_PER_CHANNEL_MAX: ${{ vars.CYBERLURCH_DETAIL_ITEMS_PER_CHANNEL_MAX || '1' }}" in text
    assert "CYBERLURCH_DEEPDIVE_MIN_TEXT_CHARS: ${{ vars.CYBERLURCH_DEEPDIVE_MIN_TEXT_CHARS || '2500' }}" in text
    assert "CYBERLURCH_DIRECT_TRANSCRIPT_MAX_CHARS: ${{ vars.CYBERLURCH_DIRECT_TRANSCRIPT_MAX_CHARS || '80000' }}" in text
    assert "CYBERLURCH_TRANSCRIPT_CHUNKING_MIN_CHARS: ${{ vars.CYBERLURCH_TRANSCRIPT_CHUNKING_MIN_CHARS || '80000' }}" in text
