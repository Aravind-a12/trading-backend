from fastapi import APIRouter, Query, HTTPException, BackgroundTasks
from app.services.forex_calendar_service import (
    get_events as get_forex_events_service,
    get_original_events as get_original_forex_events_service,
    delete_database_records as delete_database_records_service,
    database_info as get_database_info_service,
    health_check as health_check_service
)

router = APIRouter()

@router.get("/", response_model=dict)
async def root():
    """API status and information"""
    return {
        "message": "Forex Factory Scraper API with Paraphrasing",
        "version": "1.0.0",
        "status": "running",
        "features": {
            "paraphrasing": "Enabled - Returns paraphrased data by default",
            "dual_database": "Original and paraphrased data stored separately in database",
            "flexible_output": "Choose between original or paraphrased data"
        },
        "endpoints": {
            "/events": "GET - Retrieve forex events (paraphrased by default, use ?original=true for original)",
            "/events/original": "GET - Retrieve original forex events (same as /events?original=true)",
            "/health": "GET - Health check",
            "/database/delete": "DELETE - Delete database records for date range",
            "/database/info": "GET - Database statistics"
        },
        "usage_examples": {
            "paraphrased": "/events?start=2025-08-16&end=2025-08-17",
            "original": "/events?start=2025-08-16&end=2025-08-17&original=true",
            "original_alt": "/events/original?start=2025-08-16&end=2025-08-17"
        }
    }

@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return await health_check_service()

@router.get("/events")
async def get_events(
    start: str = Query(..., description="Start date in YYYY-MM-DD format", examples=["2025-08-16"]),
    end: str = Query(..., description="End date in YYYY-MM-DD format", examples=["2025-08-17"]),
    original: bool = Query(False, description="Return original data instead of paraphrased", examples=[False]),
    background_tasks: BackgroundTasks = None
):
    """Get forex events for specified date range"""
    return await get_forex_events_service(start, end, original, background_tasks)

@router.get("/events/original")
async def get_original_events(
    start: str = Query(..., description="Start date in YYYY-MM-DD format", examples=["2025-08-16"]),
    end: str = Query(..., description="End date in YYYY-MM-DD format", examples=["2025-08-17"]),
    background_tasks: BackgroundTasks = None
):
    """Get original (non-paraphrased) forex events for specified date range"""
    return await get_original_forex_events_service(start, end, background_tasks)

@router.delete("/database/delete")
async def delete_database_records(
    start: str = Query(..., description="Start date in YYYY-MM-DD format", examples=["2025-08-16"]),
    end: str = Query(..., description="End date in YYYY-MM-DD format", examples=["2025-08-17"])
):
    """Delete database records for specified date range"""
    return await delete_database_records_service(start, end)

@router.get("/database/info")
async def database_info():
    """Get database information and statistics"""
    return await get_database_info_service()
