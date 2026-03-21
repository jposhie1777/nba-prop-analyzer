#!/usr/bin/env python3
import os
import json
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.responses import JSONResponse
from google.cloud import bigquery
 
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "YOUR_PROJECT_ID")
API_TOKEN = os.environ.get("MCP_API_TOKEN", "")  # set this in Cloud Run env vars
 
bq = bigquery.Client(project=PROJECT_ID)
app = FastAPI()
 
 
# ---------------------------
# AUTH
# ---------------------------
def verify_token(request: Request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or auth[7:] != API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")
 
 
# ---------------------------
# TOOLS DEFINITION
# ---------------------------
TOOLS = [
    {
        "name": "list_datasets",
        "description": "List all BigQuery datasets in the project",
        "inputSchema": {"type": "object", "properties": {}}
    },
    {
        "name": "list_tables",
        "description": "List all tables in a BigQuery dataset",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dataset": {"type": "string", "description": "The dataset ID"}
            },
            "required": ["dataset"]
        }
    },
    {
        "name": "get_table_schema",
        "description": "Get the schema (columns and types) for a BigQuery table",
        "inputSchema": {
            "type": "object",
            "properties": {
                "dataset": {"type": "string", "description": "The dataset ID"},
                "table": {"type": "string", "description": "The table ID"}
            },
            "required": ["dataset", "table"]
        }
    },
    {
        "name": "run_query",
        "description": "Execute a read-only SELECT query against BigQuery",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "A SELECT SQL query to execute"},
                "limit": {"type": "integer", "description": "Max rows to return (default 100, max 1000)"}
            },
            "required": ["sql"]
        }
    }
]
 
 
# ---------------------------
# TOOL EXECUTION
# ---------------------------
def execute_tool(tool: str, args: dict) -> dict:
    if tool == "list_datasets":
        datasets = list(bq.list_datasets())
        return {"datasets": [d.dataset_id for d in datasets]}
 
    elif tool == "list_tables":
        tables = list(bq.list_tables(args["dataset"]))
        return {"tables": [t.table_id for t in tables]}
 
    elif tool == "get_table_schema":
        ref = f"{PROJECT_ID}.{args['dataset']}.{args['table']}"
        tbl = bq.get_table(ref)
        return {
            "schema": [
                {"name": f.name, "type": f.field_type, "mode": f.mode}
                for f in tbl.schema
            ]
        }
 
    elif tool == "run_query":
        sql = args.get("sql", "").strip()
        if not sql.lower().startswith("select"):
            return {"error": "Only SELECT queries are allowed (read-only mode)."}
        limit = min(int(args.get("limit", 100)), 1000)
        if "limit" not in sql.lower():
            sql = f"{sql} LIMIT {limit}"
        rows = [dict(row) for row in bq.query(sql).result()]
        return {"rows": rows, "row_count": len(rows)}
 
    else:
        return {"error": f"Unknown tool: {tool}"}
 
 
# ---------------------------
# MCP ENDPOINT
# ---------------------------
@app.post("/mcp", dependencies=[Depends(verify_token)])
async def mcp_handler(request: Request):
    try:
        req = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON"})
 
    method = req.get("method")
    req_id = req.get("id")
 
    # Handshake
    if method == "initialize":
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "bq-mcp-server", "version": "1.0.0"}
            }
        })
 
    # Notifications — acknowledge silently
    if method and method.startswith("notifications/"):
        return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": {}})
 
    # List tools
    if method == "tools/list":
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"tools": TOOLS}
        })
 
    # Call tool
    if method == "tools/call":
        tool = req["params"]["name"]
        args = req["params"].get("arguments", {})
        try:
            result = execute_tool(tool, args)
        except Exception as e:
            result = {"error": str(e)}
 
        return JSONResponse({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "content": [{"type": "text", "text": json.dumps(result, default=str)}]
            }
        })
 
    # Unknown method
    return JSONResponse({
        "jsonrpc": "2.0",
        "id": req_id,
        "error": {"code": -32601, "message": f"Unknown method: {method}"}
    })
 
 
# ---------------------------
# HEALTH CHECK
# ---------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}