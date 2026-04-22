import { neon } from "@neondatabase/serverless";

export const config = { runtime: "edge" };

const CLOUD_RUN_HOST = "https://mobile-api-763243624328.us-central1.run.app";
const ENDPOINT = "/mlb/matchups/nrfi";

export default async function handler(req: Request): Promise<Response> {
  const started = Date.now();
  const url = new URL(req.url);
  const state = (url.searchParams.get("state") ?? "nj").toLowerCase();
  const paramsHash = `state:${state}`;

  const dbUrl = process.env.NEON_DATABASE_URL;
  if (dbUrl) {
    try {
      const sql = neon(dbUrl);
      const rows = (await sql`
        SELECT payload, refreshed_at
        FROM mlb_api_cache
        WHERE endpoint = ${ENDPOINT}
          AND params_hash = ${paramsHash}
        ORDER BY cache_date DESC, refreshed_at DESC
        LIMIT 1
      `) as Array<{ payload: unknown; refreshed_at: string }>;

      if (rows.length > 0) {
        return new Response(JSON.stringify(rows[0].payload), {
          status: 200,
          headers: {
            "Content-Type": "application/json",
            "x-pulse-cache-source": "neon",
            "x-pulse-cache-refreshed-at": rows[0].refreshed_at,
            "x-pulse-cache-ms": String(Date.now() - started),
          },
        });
      }
    } catch (err) {
      console.error("neon read failed:", err);
    }
  }

  const upstreamUrl = `${CLOUD_RUN_HOST}${ENDPOINT}${url.search}`;
  try {
    const upstream = await fetch(upstreamUrl);
    const body = await upstream.arrayBuffer();
    return new Response(body, {
      status: upstream.status,
      headers: {
        "Content-Type":
          upstream.headers.get("content-type") ?? "application/json",
        "x-pulse-cache-source": "cloud-run-fallback",
        "x-pulse-cache-ms": String(Date.now() - started),
      },
    });
  } catch (err) {
    return new Response(
      JSON.stringify({ error: err instanceof Error ? err.message : String(err) }),
      { status: 500, headers: { "Content-Type": "application/json" } },
    );
  }
}
