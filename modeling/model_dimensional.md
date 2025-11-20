erDiagram

    DimTempo {
        INTEGER id_tempo PK
        DATE data_imagem
        INTEGER ano
        INTEGER mes
        INTEGER dia
        STRING ano_mes
        INTEGER semestre
    }

    DimLocalidade {
        INTEGER id_localidade PK
        STRING estado
    }

    DimDegradacao {
        INTEGER id_degradacao PK
        STRING tipo_degradacao
    }

    FatoDesmatamento {
        INTEGER id_fato PK
        INTEGER fk_tempo FK
        INTEGER fk_localidade FK
        INTEGER fk_degradacao FK
        FLOAT area_km
    }

    %% Relacionamentos
    DimTempo ||--o{ FatoDesmatamento : "fk_tempo → id_tempo"
    DimLocalidade ||--o{ FatoDesmatamento : "fk_localidade → id_localidade"
    DimDegradacao ||--o{ FatoDesmatamento : "fk_degradacao → id_degradacao"
