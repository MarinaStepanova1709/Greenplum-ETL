8CREATE OR REPLACE FUNCTION std12_116_pr.f_create_date_partition(
  p_table_name      text,       -- 'schema.table'
  p_partition_value timestamp   -- любая дата внутри нужного месяца
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_schema        text := split_part(p_table_name, '.', 1);
  v_table         text := split_part(p_table_name, '.', 2);

  -- первый день целевого месяца
  v_target_month  date := date_trunc('month', p_partition_value)::date;

  -- максимальная существующая YYYYMM-партиция
  v_existing_max  date;

  v_current_month date;
  v_next_month    date;
  v_part_name     text;
  v_sql           text;
  v_exists        boolean;
BEGIN
  ------------------------------------------------------------------
  -- 1) Определяем, до какого месяца уже есть table_YYYYMM
  ------------------------------------------------------------------
  SELECT max(to_date(substring(c.relname FROM length(v_table)+2), 'YYYYMM'))
  INTO v_existing_max
  FROM pg_class c
    JOIN pg_namespace n ON n.oid      = c.relnamespace
    JOIN pg_inherits i  ON i.inhrelid = c.oid
  WHERE i.inhparent = p_table_name::regclass
    AND c.relname   ~ ('^' || v_table || '_[0-9]{6}$');

  IF v_existing_max IS NULL THEN
    v_current_month := v_target_month;
    RAISE NOTICE 'Нет YYYYMM-партиций, начинаем с %', v_current_month;
  ELSE
    v_current_month := v_existing_max + interval '1 month';
    RAISE NOTICE 'Последняя партиция: % → стартуем с %',
                 v_existing_max, v_current_month;
  END IF;

  ------------------------------------------------------------------
  -- 2) Цикл: для каждого месяца до целевого – SPLIT DEFAULT PARTITION
  ------------------------------------------------------------------
  WHILE v_current_month <= v_target_month LOOP
    v_next_month := v_current_month + interval '1 month';
    v_part_name  := v_table || '_' || to_char(v_current_month, 'YYYYMM');

    -- проверяем, существует ли уже такая партиция
    SELECT EXISTS(
      SELECT 1
      FROM pg_inherits inh
      JOIN pg_class child ON child.oid = inh.inhrelid
      WHERE inh.inhparent = p_table_name::regclass
        AND child.relname = v_part_name
    )
    INTO v_exists;

    IF NOT v_exists THEN
      v_sql := format($fmt$
        ALTER TABLE %I.%I
          SPLIT DEFAULT PARTITION
            START (%L) INCLUSIVE
            END   (%L) EXCLUSIVE
          INTO (PARTITION %I, DEFAULT PARTITION)
      $fmt$,
        v_schema,
        v_table,
        v_current_month::text,
        v_next_month::text,
        v_part_name
      );

      RAISE NOTICE 'Выполняем DDL: %', v_sql;
      EXECUTE v_sql;
    ELSE
      RAISE NOTICE 'Партиция % уже существует, пропускаем', v_part_name;
    END IF;

    v_current_month := v_next_month;
  END LOOP;

  RAISE NOTICE 'Готово: созданы все месячные партиции до %', v_target_month;
END;
$$;
