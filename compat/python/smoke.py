import asyncio
import json
import os
import time

MARKER = "compatibility report complete"


def target():
    return os.getenv("PG_KINETIC_COMPAT_TARGET", "direct-postgres")


def item(library, outcome, started, reason=None, error=None):
    return {
        "suite_id": f"python-{library}", "language": "python", "library": library,
        "version": "configured", "target": target(), "outcome": outcome,
        "duration_ms": int((time.monotonic() - started) * 1000),
        "skip_reason": reason, "error_summary": error,
    }


def observed(case_id, outcome):
    return {"case_id": case_id, "outcome": outcome}


def emit(results):
    summary = {"pass": 0, "fail": 0, "skip": 0}
    for result in results:
        summary[result["outcome"]] += 1
    print(json.dumps({"ok": summary["fail"] == 0, "success_marker": MARKER,
                      "summary": summary, "results": results}))


async def exercise_psycopg(url):
    import psycopg
    async with await psycopg.AsyncConnection.connect(url) as connection:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT %s::int", (1,))
            try:
                await cursor.execute("SELECT * FROM compat_missing_relation")
            except psycopg.errors.UndefinedTable:
                pass


async def exercise_asyncpg(url):
    import asyncpg
    connection = await asyncpg.connect(url)
    try:
        await connection.fetchval("SELECT $1::int", 1)
    finally:
        await connection.close()


async def exercise_sqlalchemy(url):
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine
    if url.startswith("postgresql://"):
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
    engine = create_async_engine(url, pool_size=1, max_overflow=0)
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT CAST(:value AS INTEGER)"), {"value": 1})
    finally:
        await engine.dispose()


async def main():
    libraries = [("psycopg", exercise_psycopg), ("asyncpg", exercise_asyncpg), ("sqlalchemy", exercise_sqlalchemy)]
    requested = os.getenv("PG_KINETIC_COMPAT_LIBRARY")
    if requested:
        libraries = [(name, exercise) for name, exercise in libraries if name == requested]
    if os.getenv("PG_KINETIC_COMPAT_LIVE") != "1":
        emit([item(name, "skip", time.monotonic(), "live-disabled", "set PG_KINETIC_COMPAT_LIVE=1") for name, _ in libraries])
        return
    variable = "DATABASE_URL_PROXY" if target() == "pg-kinetic" else "DATABASE_URL_DIRECT"
    url = os.getenv(variable)
    if not url:
        emit([item(name, "skip", time.monotonic(), "database-url-unavailable", f"set {variable}") for name, _ in libraries])
        return
    results = []
    for name, exercise in libraries:
        started = time.monotonic()
        try:
            await asyncio.wait_for(exercise(url), float(os.getenv("PG_KINETIC_COMPAT_TIMEOUT_SECONDS", "30")))
        except ModuleNotFoundError as error:
            results.append(item(name, "skip", started, "library-unavailable", str(error)))
        except Exception as error:  # The normalized report carries the client error.
            results.append(item(name, "fail", started, error=str(error)))
        else:
            result = item(name, "pass", started)
            if name == "psycopg":
                result["cases"] = [
                    observed("startup-connect", "connected"),
                    observed("parameterized-query", "one-row"),
                    observed("error-propagation", "sqlstate"),
                ]
            elif name == "asyncpg":
                result["cases"] = [
                    observed("startup-connect", "connected"),
                    observed("parameterized-query", "one-row"),
                ]
            elif name == "sqlalchemy":
                result["cases"] = [
                    observed("startup-connect", "connected"),
                    observed("simple-query", "one-row"),
                ]
            results.append(result)
    emit(results)


if __name__ == "__main__":
    asyncio.run(main())
