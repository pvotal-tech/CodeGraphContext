# src/codegraphcontext/core/__init__.py
"""
Core database management module.

Currently hardcoded to rely exclusively on Google Cloud Spanner via SpannerDBManager.
"""
import os
from codegraphcontext.utils.debug_log import info_logger
from .database_spanner import SpannerDBManager

def _is_spanner_configured() -> bool:
    """Check if Google Cloud Spanner is configured via environment variables."""
    return bool(os.getenv('SPANNER_INSTANCE_ID') and os.getenv('SPANNER_DATABASE_ID'))

def get_database_manager() -> SpannerDBManager:
    """
    Factory function hardcoded to return the Spanner database manager.
    """
    if not _is_spanner_configured():
        raise ValueError("Google Cloud Spanner is not configured.\nSet SPANNER_INSTANCE_ID and SPANNER_DATABASE_ID environment variables.")
    info_logger("Using Google Cloud Spanner Server")
    return SpannerDBManager()

__all__ = ['SpannerDBManager', 'get_database_manager']
