import { neon } from "@neondatabase/serverless";

export const config = { runtime: "edge" };

const CLOUD_RUN_FALLBACK =
  "https://mobile-api-763243624328.us-central1.run.app/mlb/matchups/cheat-sheet";

export default async function handler(_req: Request): Promise<Response> {
  const started = Date.now();
  const dbUrl = process.env.NEON_DATABASE_URL;
  if (!dbUrl) {
    return json({ error: "NEON_DATABASE_URL not configured" }, 500);
  }

  try {
    const sql = neon(dbUrl);
    const rows = (await sql`
      SELECT payload, refreshed_at
      FROM mlb_api_cache
      WHERE endpoint = '/mlb/matchups/cheat-sheet'
        AND params_hash = 'none'
      ORDER BY cache_date DESC, refreshed_at DESC
      LIMIT 1
    `) as Array<{ payload: Record<string, unknown>; refreshed_at: string }>;

    if (rows.length > 0) {
      const { payload, refreshed_at } = rows[0];
      return json(
        {
          ...payload,
          _cache: {
            source: "neon",
            refreshed_at,
            ms: Date.now() - started,
          },
        },
        200,
      );
    }
  } catch (err) {
    // fall through to Cloud Run; log for visibility
    console.error("neon read failed:", err);
  }

  try {
    const upstream = await fetch(CLOUD_RUN_FALLBACK);
    if (!upstream.ok) {
      return json(
        { error: `Upstream failed: ${upstream.status}` },
        upstream.status,
      );
    }
    const data = (await upstream.json()) as Record<string, unknown>;
    return json(
      {
        ...data,
        _cache: {
          source: "cloud-run-fallback",
          ms: Date.now() - started,
        },
      },
      200,
    );
  } catch (err) {
    return json(
      { error: err instanceof Error ? err.message : String(err) },
      500,
    );
  }
}

function json(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
