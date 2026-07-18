TRUNCATE TABLE compat_audit, compat_policy_rules, compat_items RESTART IDENTITY;

INSERT INTO compat_items (id, shard_key, name, active)
VALUES
    (1, 101, 'alpha', true),
    (2, 202, 'beta', true),
    (3, 303, 'gamma', false)
ON CONFLICT (id) DO UPDATE
SET shard_key = EXCLUDED.shard_key,
    name = EXCLUDED.name,
    active = EXCLUDED.active;

INSERT INTO compat_audit (id, item_id, note)
VALUES
    (1, 1, 'created'),
    (2, 2, 'created')
ON CONFLICT (id) DO UPDATE
SET item_id = EXCLUDED.item_id,
    note = EXCLUDED.note;

INSERT INTO compat_policy_rules (id, rule_name, action, relation_name, enabled)
VALUES
    (1, 'deny_sensitive_read', 'deny', 'compat_items', true)
ON CONFLICT (id) DO UPDATE
SET rule_name = EXCLUDED.rule_name,
    action = EXCLUDED.action,
    relation_name = EXCLUDED.relation_name,
    enabled = EXCLUDED.enabled;
