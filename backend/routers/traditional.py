from fastapi import APIRouter, Query

from backend.services import bigquery_client as bq

router = APIRouter(prefix="/api/traditional", tags=["traditional"])


@router.get("/{project_code}")
async def get_traditional_buys(project_code: str):
    sql = f"""
        SELECT
            b.buy_id,
            b.vendor_name,
            b.buy_type,
            b.station_call_sign,
            b.start_date,
            b.end_date,
            b.total_spots,
            b.net_cost,
            b.gross_cost,
            b.status
        FROM {bq.table('traditional_buys')} b
        WHERE b.project_code = @project_code
        ORDER BY b.start_date DESC
    """
    rows = bq.run_query(sql, [bq.string_param("project_code", project_code)])
    return rows


@router.get("/{project_code}/{buy_id}/lines")
async def get_traditional_buy_lines(project_code: str, buy_id: str):
    sql = f"""
        SELECT
            l.line_id,
            l.description,
            l.day_pattern,
            l.time_start,
            l.time_end,
            l.spot_length_seconds,
            l.rate,
            l.rate_type,
            l.spots_per_week,
            l.total_spots,
            l.total_cost
        FROM {bq.table('traditional_buy_lines')} l
        WHERE l.buy_id = @buy_id
        ORDER BY l.line_id
    """
    rows = bq.run_query(sql, [bq.string_param("buy_id", buy_id)])
    return rows
