import os
from dotenv import load_dotenv
import snowflake.connector
from groq import Groq

load_dotenv()

conn = snowflake.connector.connect(
    account=os.environ['SNOWFLAKE_ACCOUNT'],
    user=os.environ['SNOWFLAKE_USER'],
    password=os.environ['SNOWFLAKE_PASSWORD'],
    warehouse='SENTINEL_WH',
    database='SENTINEL_DB',
    schema='GOVERNANCE'
)
cur = conn.cursor()

cur.execute("""
    SELECT q.check_id, q.check_name, q.rows_affected, q.detail
    FROM SENTINEL_DB.GOVERNANCE.quality_log q
    WHERE q.status = 'FAIL'
      AND q.check_id NOT IN (SELECT check_id FROM SENTINEL_DB.GOVERNANCE.incident_log)
""")
failures = cur.fetchall()
print(f"Found {len(failures)} unresolved failure(s)")

cur.execute("""
    SELECT date, delinquency_rate
    FROM SENTINEL_DB.CLEAN.delinquency
    ORDER BY date DESC
    LIMIT 5
""")
recent = cur.fetchall()
context = ", ".join(f"{d}={v}" for d, v in recent)

client = Groq(api_key=os.environ['GROQ_API_KEY'])

for check_id, check_name, rows_affected, detail in failures:
    prompt = (
        f"A data quality check failed. Check: {check_name}. "
        f"Rows affected: {rows_affected}. Detail: {detail}. "
        f"Recent context: {context}. "
        "In 2-3 plain-English sentences, explain the likely cause and whether "
        "this looks like a source data error or a genuine economic signal."
    )
    response = client.chat.completions.create(
        model="openai/gpt-oss-120b",
        messages=[{"role": "user", "content": prompt}]
    )
    narrative = response.choices[0].message.content

    cur.execute("""
        INSERT INTO SENTINEL_DB.GOVERNANCE.incident_log (check_id, narrative)
        VALUES (%s, %s)
    """, (check_id, narrative))
    print(f"Logged narrative for {check_name}: {narrative[:80]}...")

conn.commit()
cur.close()
conn.close()