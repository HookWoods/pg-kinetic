CREATE TABLE IF NOT EXISTS compat_items (
    id integer PRIMARY KEY,
    shard_key integer NOT NULL,
    name text NOT NULL,
    active boolean NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS compat_audit (
    id integer PRIMARY KEY,
    item_id integer NOT NULL REFERENCES compat_items(id),
    note text NOT NULL
);

CREATE TABLE IF NOT EXISTS compat_policy_rules (
    id integer PRIMARY KEY,
    rule_name text NOT NULL UNIQUE,
    action text NOT NULL CHECK (action IN ('allow', 'deny')),
    relation_name text NOT NULL,
    enabled boolean NOT NULL DEFAULT true
);
