CREATE TABLE IF NOT EXISTS expense_classified_by_rules AS (
    WITH expense_classified_by_rules AS (
        SELECT 
            DS_DESPESA,
            CASE
                WHEN
                    ds_despesa ilike '%gasolina%'
                    OR     ds_despesa ilike '%panflet%'
                    OR     ds_despesa ilike '%veiculo%'
                    OR     ds_despesa ilike '%veículo%'
                    OR     ds_despesa ilike '%carro%'
                    OR     ds_despesa ilike '%combustivel%'
                    OR     ds_despesa ilike '%combustível%'
                    OR     ds_despesa ilike '%moto%'
                THEN 'Transport'

                WHEN 
                    ds_despesa ilike '%adesiv%'
                    OR     ds_despesa ilike '%santinho%'
                    OR     ds_despesa ilike '%bandeir%'
                    OR     ds_despesa ilike '%impress%'
                    OR     ds_despesa ilike '%botton%'
                    OR     ds_despesa ilike '%banner%'
                    OR     ds_despesa ilike '%cartaz%'
                    OR     ds_despesa ilike '%papel%' 
                    OR     ds_despesa ilike '%panfleto%'
                    OR     ds_despesa ilike '%bone%'
                    OR     ds_despesa ilike '%camisa%'
                THEN 'Printed Material'

                WHEN 
                    ds_despesa ilike '%facebook%' 
                    OR     ds_despesa ilike '%instagram%' 
                    OR     ds_despesa ilike '%tiktok%' 
                    OR     ds_despesa ilike '%youtube%'
                    OR     ds_despesa ilike '%impulsionamento de conteudo%'
                THEN 'Online Advertising'

                WHEN
                    ds_despesa ilike '%tarifa%' 
                    OR     ds_despesa ilike '%taxa%' 
                THEN 'Tariffs and taxes'

                WHEN 
                    ds_despesa ilike '%aliment%'
                THEN 'Food'

                WHEN 
                    ds_despesa ilike '%hospedagem%'
                THEN 'Physical Space'

                WHEN 
                    ds_despesa ilike '%cabo eleitoral%'
                THEN 'Rallies and Actions'

                ELSE NULL
            END AS classification 

        FROM expense_description
    )
    SELECT
        *
    FROM expense_classified_by_rules
    WHERE classification IS NOT NULL
)