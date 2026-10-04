CREATE TABLE paid_expense AS (
    SELECT
        *
    FROM read_csv(
        './data/extracted/prestacao_contas/*/*/despesas_pagas_*_BRASIL.csv',
        delim=';',
        filename=true
    )
)