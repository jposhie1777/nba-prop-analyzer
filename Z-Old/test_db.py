import psycopg2

DATABASE_URL = "postgresql://postgres.uvgpjxwuexrtiluzjuqs:Xd97QpA6mJ3tB9LwU2K4@aws-1-us-east-2.pooler.supabase.com:6543/postgres"

try:
    print("Connecting...")
    conn = psycopg2.connect(DATABASE_URL)
    print("Connected successfully!")

    cur = conn.cursor()
    cur.execute("SELECT NOW();")
    print("Database time:", cur.fetchone())

    conn.close()
    print("Connection closed.")

except Exception as e:
    print("ERROR:", e)
