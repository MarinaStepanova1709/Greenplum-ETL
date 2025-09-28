-- Создание или замена функции для полной загрузки данных в таблицу
--Аргументы - передаем название таблицы и название файла
CREATE OR REPLACE FUNCTION std12_116_pr.f_load_full(p_table text, p_file_name text)
RETURNS int4                           -- Функция возвращает количество строк int4
LANGUAGE plpgsql                       
VOLATILE                                -- Функция изменяет данные 
AS $$
DECLARE
    v_ext_table_name text;             -- Имя внешней таблицы (будет временной)
    v_sql text;                        -- Переменная для динамического SQL-запроса
    v_gpfdist text;                    -- Строка подключения к GPFDIST с файлом
    v_result int;                      -- Количество строк после загрузки
BEGIN
    -- Формируем имя внешней таблицы
    v_ext_table_name = p_table || '_ext';

    -- Очищаем основную таблицу
    EXECUTE 'TRUNCATE TABLE ' || p_table;

    -- Удаляем внешнюю таблицу, если она уже существует
    EXECUTE 'DROP EXTERNAL TABLE IF EXISTS ' || v_ext_table_name;

    -- Формируем строку подключения GPFDIST к файлу .CSV
    v_gpfdist = 'gpfdist://172.16.128.150:8080/' || p_file_name || '.CSV';
    
        -- Создаём команду для создания внешней таблицы с той же структурой, что и основная
    v_sql = 'CREATE EXTERNAL TABLE ' || v_ext_table_name || ' (LIKE ' || p_table || ')
    LOCATION (''' || v_gpfdist || ''')
    ON ALL
    FORMAT ''CSV'' (HEADER DELIMITER '';'' NULL '''' ESCAPE ''"'' QUOTE ''"'' )
    ENCODING ''UTF8'' ';
        
    -- Пишем в лог финальный текст SQL-запроса для создания внешней таблицы
    RAISE NOTICE 'Текст SQL for EXTERNAL TABLE IS: %', v_sql;

    -- Создаём внешнюю таблицу
    EXECUTE v_sql;

    -- Загружаем данные из внешней таблицы во внутреннюю
    EXECUTE 'INSERT INTO ' || p_table || ' SELECT * FROM ' || v_ext_table_name;

    -- Считаем количество строк в основной таблице после загрузки
    EXECUTE 'SELECT COUNT(1) FROM ' || p_table
	INTO v_result;

    -- Возвращаем количество загруженных строк
    RETURN v_result;
END;
$$
EXECUTE ON ANY;    


SELECT std12_116_pr.f_load_full('std12_116_pr.promotions', 'promotions');

select * from std12_116_pr.promotions;

select std12_116.f_load_full('std12_116_pr.stores', 'stores');

select std12_116.f_load_full('std12_116_pr.promo_types', 'promo_types');









select * from std12_116_pr.stores;

select * from std12_116.price; 
