#!/usr/bin/env python3
import sys
import json
import asyncio
from google.cloud import bigquery
 
PROJECT_ID = "graphite-flare-477419-h7"  # <-- replace this
 
bq = bigquery.Client(project=PROJECT_ID)
 
 
async def send(msg):
    sys.stdout.write(json.dumps(msg) + "\n")
    sys.stdout.flush()
 
 
async def main():
    while True:
        line = await asyncio.get_event_loop().run_in_executor(None, sys.stdin.readline)
        if not line:
            break
 
        try:
            req = json.loads(line)
        except Exception:
            continue
 
        method = req.get("method")
        req_id = req.get("id")
 
        # ---------------------------
        # INITIALIZE (required handshake)
        # ---------------------------
        if method == "initialize":
            await send({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "bq-server", "version": "1.0.0"}
                }
            })
            continue
 
        # ---------------------------
        # NOTIFICATIONS (no response needed)
        # ---------------------------
        if method and method.startswith("notifications/"):
            continue
 
        # ---------------------------
        # LIST TOOLS
        # ---------------------------
        if method == "tools/list":
            await send({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "tools": [
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
                                    "dataset": {
                                        "type": "string",
                                        "description": "The dataset ID to list tables from"
                                    }
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
                                    "dataset": {
                                        "type": "string",
                                        "description": "The dataset ID"
                                    },
                                    "table": {
                                        "type": "string",
                                        "description": "The table ID"
                                    }
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
                                    "sql": {
                                        "type": "string",
                                        "description": "A SELECT SQL query to execute"
                                    },
                                    "limit": {
                                        "type": "integer",
                                        "description": "Max rows to return (default 100, max 1000)"
                                    }
                                },
                                "required": ["sql"]
                            }
                        }
                    ]
                }
            })
            continue
 
        # ---------------------------
        # CALL TOOL
        # ---------------------------
        if method == "tools/call":
            tool = req["params"]["name"]
            args = req["params"].get("arguments", {})
 
            try:
                # list_datasets
                if tool == "list_datasets":
                    datasets = list(bq.list_datasets())
                    result = {"datasets": [d.dataset_id for d in datasets]}
 
                # list_tables
                elif tool == "list_tables":
                    dataset = args["dataset"]
                    tables = list(bq.list_tables(dataset))
                    result = {"tables": [t.table_id for t in tables]}
 
                # get_table_schema
                elif tool == "get_table_schema":
                    dataset = args["dataset"]
                    table = args["table"]
                    ref = f"{PROJECT_ID}.{dataset}.{table}"
                    tbl = bq.get_table(ref)
                    result = {
                        "schema": [
                            {"name": f.name, "type": f.field_type, "mode": f.mode}
                            for f in tbl.schema
                        ]
                    }
 
                # run_query (read-only)
                elif tool == "run_query":
                    sql = args.get("sql", "").strip()
                    if not sql.lower().startswith("select"):
                        result = {"error": "Only SELECT queries are allowed (read-only mode)."}
                    else:
                        limit = min(int(args.get("limit", 100)), 1000)
                        # Wrap in a limit if not already present
                        if "limit" not in sql.lower():
                            sql = f"{sql} LIMIT {limit}"
                        rows = [dict(row) for row in bq.query(sql).result()]
                        result = {"rows": rows, "row_count": len(rows)}
 
                else:
                    result = {"error": f"Unknown tool: {tool}"}
 
            except Exception as e:
                result = {"error": str(e)}
 
            await send({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": json.dumps(result, default=str)}]
                }
            })
            continue
 
        # ---------------------------
        # UNKNOWN METHOD
        # ---------------------------
        await send({
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"Unknown method: {method}"}
        })
 
 
if __name__ == "__main__":
    asyncio.run(main())