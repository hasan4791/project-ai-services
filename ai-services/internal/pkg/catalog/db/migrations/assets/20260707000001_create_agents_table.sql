-- +goose Up
-- +goose StatementBegin
-- Tracks registered remote worker agents.
CREATE TABLE agents (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_name     TEXT        NOT NULL UNIQUE,
    labels         JSONB       NOT NULL DEFAULT '{}',
    capabilities   JSONB       NOT NULL DEFAULT '{}',
    status         TEXT        NOT NULL DEFAULT 'pending',
    -- status values: pending | ready | busy | draining | disconnected | rejected
    last_heartbeat TIMESTAMPTZ,
    registered_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_agents_status     ON agents (status);
CREATE INDEX idx_agents_agent_name ON agents (agent_name);

-- Reuse the existing update_updated_at_column() trigger function.
CREATE TRIGGER update_agents_updated_at
    BEFORE UPDATE ON agents
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER IF EXISTS update_agents_updated_at ON agents;
DROP INDEX  IF EXISTS idx_agents_agent_name;
DROP INDEX  IF EXISTS idx_agents_status;
DROP TABLE  IF EXISTS agents;
-- +goose StatementEnd
