CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

-- 1. Создаем функцию для логирования изменений пользователей
CREATE OR REPLACE FUNCTION log_user_changes()
RETURNS TRIGGER AS $$
BEGIN
    -- Логируем изменения имени
    IF OLD.name IS DISTINCT FROM NEW.name THEN
        INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'name', OLD.name, NEW.name);
    END IF;
    
    -- Логируем изменения email
    IF OLD.email IS DISTINCT FROM NEW.email THEN
        INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'email', OLD.email, NEW.email);
    END IF;
    
    -- Логируем изменения роли
    IF OLD.role IS DISTINCT FROM NEW.role THEN
        INSERT INTO users_audit (user_id, changed_by, field_changed, old_value, new_value)
        VALUES (OLD.id, current_user, 'role', OLD.role, NEW.role);
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Создаем триггер на таблицу users
CREATE OR REPLACE TRIGGER users_changes_trigger
AFTER UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_user_changes();

-- 3. Устанавливаем расширение pg_cron
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 4. Создаем функцию для экспорта свежих данных в CSV
CREATE OR REPLACE FUNCTION export_daily_audit()
RETURNS VOID AS $$
DECLARE
    export_path TEXT;
    export_date TEXT;
BEGIN
    -- Формируем путь к файлу с текущей датой
    export_date := TO_CHAR(CURRENT_DATE, 'YYYY_MM_DD');
    export_path := '/tmp/users_audit_export_' || export_date || '.csv';
    
    -- Экспортируем данные за сегодняшний день
    EXECUTE format('COPY (
        SELECT * FROM users_audit 
        WHERE changed_at >= CURRENT_DATE 
        AND changed_at < CURRENT_DATE + INTERVAL ''1 day''
    ) TO %L WITH CSV HEADER', export_path);
END;
$$ LANGUAGE plpgsql;

-- 5. Настраиваем планировщик pg_cron на запуск в 3:00 ночи
SELECT cron.schedule(
    'daily_audit_export',       -- название задания
    '0 3 * * *',                -- каждый день в 3:00 ночи
    'SELECT export_daily_audit()' -- выполняемая функция
);