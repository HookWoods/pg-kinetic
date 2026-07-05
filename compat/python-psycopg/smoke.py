import os

import psycopg


def main() -> None:
    url = os.environ.get(
        "DATABASE_URL",
        "postgres://postgres:postgres@127.0.0.1:58432/pgkinetic",
    )
    with psycopg.connect(url) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "select balance_cents from accounts where email = %s",
                ("alice@example.com",),
            )
            row = cursor.fetchone()

    if row is None:
        raise RuntimeError("expected one row")
    if row[0] != 1000:
        raise RuntimeError(f"expected balance 1000, got {row[0]}")

    print("python psycopg smoke passed")


if __name__ == "__main__":
    main()
