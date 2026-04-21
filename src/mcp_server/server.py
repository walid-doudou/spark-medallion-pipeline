import duckdb
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("NYC Taxi Analytics")

conn = duckdb.connect()
conn.execute("INSTALL httpfs")
conn.execute("LOAD httpfs")
conn.execute("SET s3_endpoint='localhost:4566'")
conn.execute("SET s3_access_key_id='test'")
conn.execute("SET s3_secret_access_key='test'")
conn.execute("SET s3_use_ssl=false")
conn.execute("SET s3_url_style='path'")


@mcp.tool()
def get_hourly_trips() -> str:
    """Get number of trips per hour of the day"""
    result = conn.execute(
        "SELECT * FROM delta_scan('s3://nyc-taxi/gold/hourly_trips')"
    ).df()
    return result.to_string()


@mcp.tool()
def get_revenue_by_day() -> str:
    """Get number of trips per hour of the day"""
    result = conn.execute(
        "SELECT * FROM delta_scan('s3://nyc-taxi/gold/revenue_by_day')"
    ).df()
    return result.to_string()


@mcp.tool()
def get_revenue_by_vendor() -> str:
    """Get number of trips per hour of the day"""
    result = conn.execute(
        "SELECT * FROM delta_scan('s3://nyc-taxi/gold/revenue_by_vendor')"
    ).df()
    return result.to_string()


if __name__ == "__main__":
    mcp.run()
