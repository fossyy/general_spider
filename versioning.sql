CREATE DATABASE general_spider_4;

CREATE TABLE configs (
    id uuid NOT NULL PRIMARY KEY,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    deleted_at timestamp with time zone,
    domain character varying(255) NOT NULL,
    type character varying(12) NOT NULL,
    name character varying(255) NOT NULL UNIQUE,
    description text,
    config_version INT DEFAULT 1,
    group_id character varying(10),
    data bytea
);

CREATE TABLE dashboard (
    id SERIAL PRIMARY KEY,
    key_name TEXT NOT NULL UNIQUE,
    dashboard_version TEXT DEFAULT '1.0',
    full_version TEXT
);

-- Fungsi untuk memperbarui full_version dan increment config_version
CREATE OR REPLACE FUNCTION update_dashboard_version()
RETURNS TRIGGER AS $$
DECLARE
    current_config_version INT;
BEGIN

    SELECT c.config_version INTO current_config_version
    FROM configs c
    WHERE c.name = NEW.name;

    UPDATE dashboard
    SET full_version = CONCAT(dashboard_version, '.', current_config_version)
    WHERE key_name = NEW.name;

    IF NOT FOUND THEN
        INSERT INTO dashboard (key_name, full_version)
        VALUES (
            NEW.name,
            CONCAT('1.0.', current_config_version)
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER after_update_config_version
AFTER INSERT OR UPDATE ON configs
FOR EACH ROW
EXECUTE FUNCTION update_dashboard_version();

DROP TRIGGER after_update_config_version ON configs;

CREATE OR REPLACE FUNCTION update_full_version_on_dashboard_update()
RETURNS TRIGGER AS $$
DECLARE
    current_config_version INT;
BEGIN
    SELECT c.config_version INTO current_config_version
    FROM configs c
    WHERE c.name = NEW.key_name;

    NEW.full_version := CONCAT(NEW.dashboard_version, '.', current_config_version);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER after_update_dashboard_version
BEFORE UPDATE OF dashboard_version ON dashboard
FOR EACH ROW
EXECUTE FUNCTION update_full_version_on_dashboard_update();

DROP TRIGGER after_update_dashboard_version ON dashboard;


CREATE OR REPLACE FUNCTION increment_config_version()
RETURNS TRIGGER AS $$
BEGIN
    NEW.config_version := NEW.config_version + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER after_update_config_version_increment
BEFORE UPDATE ON configs
FOR EACH ROW
EXECUTE FUNCTION increment_config_version();

DROP TRIGGER after_update_config_version_increment ON configs;

INSERT INTO configs (id, created_at, updated_at, domain, type, name, description, config_version, data)
VALUES (gen_random_uuid(), NOW(), NOW(), 'www.example1.com', 'service', 'general_spider_1', 'First config', DEFAULT, DECODE('testdata', 'escape'));

INSERT INTO dashboard (key_name, dashboard_version) VALUES ('general_spider_1', '1.0');

UPDATE configs
SET updated_at = NOW()
WHERE name = 'general_spider_1';

INSERT INTO configs (id, created_at, updated_at, domain, type, name, description, config_version, data)
VALUES (gen_random_uuid(), NOW(), NOW(), 'www.example2.com', 'service', 'general_spider_2', 'Second config', 1, DECODE('testdata', 'escape'));

UPDATE dashboard
SET dashboard_version = '2.1'
WHERE key_name = 'general_spider_1';

SELECT * FROM dashboard;
SELECT * FROM configs;

TRUNCATE TABLE dashboard;
TRUNCATE TABLE configs;

SELECT
    dashboard.id,
    dashboard.key_name,
    dashboard.dashboard_version,
    configs.config_version,
    dashboard.full_version
FROM
    dashboard
JOIN
    configs
ON
    dashboard.key_name = configs.name;

SELECT dashboard.id, dashboard.key_name, dashboard.dashboard_version, configs.config_version, dashboard.full_version
FROM dashboard
LEFT JOIN configs ON dashboard.key_name = configs.name;



-- CREATE DATABASE general_spider_3;
--
-- CREATE TABLE configs (
--     id uuid NOT NULL PRIMARY KEY,
--     created_at timestamp with time zone,
--     updated_at timestamp with time zone,
--     deleted_at timestamp with time zone,
--     domain character varying(255) NOT NULL,
--     type character varying(12) NOT NULL,
--     name character varying(255) NOT NULL UNIQUE,
--     description text,
--     config_version TEXT DEFAULT '1',
--     data bytea
-- );
--
-- CREATE TABLE dashboard (
--     id SERIAL PRIMARY KEY,
--     key_name TEXT NOT NULL UNIQUE,
--     dashboard_version TEXT DEFAULT '1.0',
--     full_version TEXT
-- );
--
-- -- Fungsi untuk memperbarui full_version berdasarkan config_version
-- CREATE OR REPLACE FUNCTION update_dashboard_version()
-- RETURNS TRIGGER AS $$
-- DECLARE
--     current_config_version TEXT;
-- BEGIN
--     SELECT c.config_version INTO current_config_version
--     FROM configs c
--     WHERE c.name = NEW.name;
--
--     UPDATE dashboard
--     SET full_version = CONCAT(dashboard_version, '.', current_config_version)
--     WHERE key_name = NEW.name;
--
--     IF NOT FOUND THEN
--         INSERT INTO dashboard (key_name, full_version)
--         VALUES (
--             NEW.name,
--             CONCAT('1.0.', current_config_version)
--         );
--     END IF;
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
--
-- -- Trigger untuk memperbarui full_version setelah INSERT atau UPDATE di tabel configs
-- CREATE TRIGGER after_update_config_version
-- AFTER INSERT OR UPDATE ON configs
-- FOR EACH ROW
-- EXECUTE FUNCTION update_dashboard_version();
--
-- -- Fungsi untuk memperbarui full_version saat dashboard_version diperbarui
-- CREATE OR REPLACE FUNCTION update_full_version_on_dashboard_update()
-- RETURNS TRIGGER AS $$
-- DECLARE
--     current_config_version TEXT;
-- BEGIN
--     SELECT c.config_version INTO current_config_version
--     FROM configs c
--     WHERE c.name = NEW.key_name;
--
--     NEW.full_version := CONCAT(NEW.dashboard_version, '.', current_config_version);
--
--     RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- -- Trigger untuk memperbarui full_version setelah UPDATE di tabel dashboard
-- CREATE TRIGGER after_update_dashboard_version
-- BEFORE UPDATE OF dashboard_version ON dashboard
-- FOR EACH ROW
-- EXECUTE FUNCTION update_full_version_on_dashboard_update();
--
--
-- INSERT INTO configs (id, created_at, updated_at, domain, type, name, description, config_version, data)
-- VALUES (gen_random_uuid(), NOW(), NOW(), 'www.example1.com', 'service', 'general_spider_1', 'First config', DEFAULT, DECODE('testdata', 'escape'));
--
-- INSERT INTO dashboard (key_name, dashboard_version) VALUES ('general_spider_1', '1.0');
--
-- UPDATE configs
-- SET config_version = '2',
--     updated_at = NOW()
-- WHERE name = 'general_spider_1';
--
-- INSERT INTO configs (id, created_at, updated_at, domain, type, name, description, config_version, data)
-- VALUES (gen_random_uuid(), NOW(), NOW(), 'www.example2.com', 'service', 'general_spider_2', 'Second config', '1', DECODE('testdata', 'escape'));
--
-- UPDATE dashboard
-- SET dashboard_version = '2.0'
-- WHERE key_name = 'general_spider_1';
--
-- SELECT * FROM dashboard;
-- SELECT * FROM configs;
--
-- TRUNCATE TABLE dashboard;
-- TRUNCATE TABLE configs;
--
-- SELECT
--     dashboard.id,
--     dashboard.key_name,
--     dashboard.dashboard_version,
--     configs.config_version,
--     dashboard.full_version
-- FROM
--     dashboard
-- JOIN
--     configs
-- ON
--     dashboard.key_name = configs.name;
