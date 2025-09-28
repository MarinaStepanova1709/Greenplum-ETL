
CREATE OR REPLACE FUNCTION std12_116.f_create_date_partition(
  p_table_name      text,       -- 'schema.table', например 'std12_116.bills_item'
  p_partition_value timestamp   -- любая дата внутри нужного месяца, например '2021-03-15'
)
RETURNS void
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
  --  имя схемы и таблицы
  v_schema       text := split_part(p_table_name, '.', 1);
  v_table        text := split_part(p_table_name, '.', 2);

  -- Вычисляем первый день целевого месяца
  v_target_month date := date_trunc('month', p_partition_value)::date;

  -- Максимальная существующая граница партиций (как текст)
  v_max_boundary text;
  -- Переведённая в дату максимальная граница
  v_max_date     date;

  -- Переменные для начала и конца разбивки
  v_split_start  date;
  v_split_end    date;

  -- Текст SQL-команды
  v_sql          text;
BEGIN
  -- 1) Получаем последнюю границу из pg_partitions
  RAISE NOTICE 'Fetching last partition boundary for table %', p_table_name;
  SELECT partitionrangeend
    INTO v_max_boundary
  FROM pg_partitions p
  WHERE p.schemaname = v_schema
    AND p.tablename  = v_table
    AND p.partitionrank IS NOT NULL
  ORDER BY partitionrank DESC
  LIMIT 1;

  -- 2) Определяем стартовую точку для создания новых партиций
  IF v_max_boundary IS NULL THEN
    v_split_start := v_target_month;
    RAISE NOTICE 'No existing partitions found, starting at %', v_split_start;
  ELSE
    -- Преобразуем текстовое выражение в дату
    EXECUTE 'SELECT ' || v_max_boundary INTO v_max_date;
    v_split_start := v_max_date;
    RAISE NOTICE 'Last partition boundary % found, starting next at %', v_max_date, v_split_start;
  END IF;

  -- 3) Цикл создания партиций до целевого месяца
  WHILE v_split_start < v_target_month LOOP
    v_split_end := (v_split_start + INTERVAL '1 month')::date;
    v_sql := format(
      'ALTER TABLE %I.%I SPLIT DEFAULT PARTITION ' ||
      'START (DATE %L) INCLUSIVE END (DATE %L) EXCLUSIVE',
      v_schema,
      v_table,
      v_split_start,
      v_split_end
    );
    RAISE NOTICE 'Executing partition split: %', v_sql;
    EXECUTE v_sql;
    -- Переводим указатель на следующий месяц
    v_split_start := v_split_end;
    RAISE NOTICE 'Partition created up to %', v_split_start;
  END LOOP;

  -- 4) Завершающее сообщение
  RAISE NOTICE 'All partitions created up to %', v_target_month;
END;
$$;
EXECUTE ON ANY;
