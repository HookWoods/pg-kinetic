import pg from "pg";

const connectionString =
  process.env.DATABASE_URL ??
  "postgres://postgres:postgres@127.0.0.1:58432/pgkinetic";

const client = new pg.Client({ connectionString });

await client.connect();
const result = await client.query({
  name: "account-balance",
  text: "select balance_cents from accounts where email = $1",
  values: ["alice@example.com"],
});
await client.end();

const balance = Number(result.rows[0].balance_cents);
if (balance !== 1000) {
  throw new Error(`expected balance 1000, got ${balance}`);
}

console.log("node pg smoke passed");
