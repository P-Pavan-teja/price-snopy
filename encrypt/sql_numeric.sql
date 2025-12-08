WITH enc AS (
    SELECT
        -- get just the ciphertext field (BINARY inside a VARIANT)
        ENCRYPT_RAW(
            TO_BINARY('123', 'UTF-8'),
            SHA2_BINARY('PAVAN TEJA', 256),
            TO_BINARY('5678987656789987', 'UTF-8'),
            NULL,
            'AES-CBC'
        ):ciphertext AS ct
),
hexval AS (
    SELECT 
        TO_VARCHAR(ct::BINARY, 'HEX') AS hexstr
    FROM enc
)
SELECT
    hexstr,
    TO_DECIMAL(
        hexstr,
        REPEAT('X', LENGTH(hexstr))    -- dynamic hex format mask
    ) AS numeric_ciphertext
FROM hexval;

select TO_VARCHAR('6B277594B1FA144475E96EFF88EE05D7'::BINARY,'HEX');

SELECT
    '6B277594B1FA144475E96EFF88EE05D7',
    TO_DECIMAL(
        '6B277594B1FA144475E96EFF88EE05D7',
        REPEAT('X', LENGTH('6B277594B1FA144475E96EFF88EE05D7'))    -- dynamic hex format mask
    ) AS numeric_ciphertext;
-- FROM hexval;

WITH enc AS (
    SELECT ENCRYPT_RAW(
        TO_BINARY('123', 'UTF-8'),
        SHA2_BINARY('PAVAN TEJA', 256),
        TO_BINARY('5678987656789987', 'UTF-8'),
        NULL,
        'AES-CBC'
    ):ciphertext AS ct
),
hexval AS (
    SELECT TO_VARCHAR(ct::BINARY, 'HEX') AS hexstr FROM enc
)
SELECT
    hexstr,
    TO_DECIMAL(
        hexstr,
        REPEAT('X',length(hexstr))
    ) AS numeric_ciphertext
FROM hexval;
