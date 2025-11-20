-- ATIVAR VERIFICACAO DE FOREIGN KEYS, VERIFICAR SE RODOU!
PRAGMA foreign_keys = ON;

-- DIMENSÃO TEMPO

CREATE TABLE IF NOT EXISTS DimTempo (
    id_tempo INTEGER PRIMARY KEY AUTOINCREMENT,
    data_imagem DATE NOT NULL,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    ano_mes TEXT NOT NULL,
    semestre INTEGER NOT NULL
);

-- DIMENSÃO LOCALIDADE

CREATE TABLE IF NOT EXISTS DimLocalidade (
    id_localidade INTEGER PRIMARY KEY AUTOINCREMENT,
    estado TEXT NOT NULL UNIQUE
);  

-- DIMENSÃO TIPO DE DEGRADAÇÃO

CREATE TABLE IF NOT EXISTS DimTipoDegradacao (
    id_tipo INTEGER PRIMARY KEY AUTOINCREMENT,
    tipo_degradacao TEXT NOT NULL UNIQUE
);

-- TABELA FATO 

CREATE TABLE IF NOT EXISTS FatoDesmatamento (
    id_fato INTEGER PRIMARY KEY AUTOINCREMENT,

    -- FOREIGN KEYS
    id_tempo INTEGER NOT NULL,
    id_localidade INTEGER NOT NULL,
    id_tipo INTEGER NOT NULL,

    -- MEDIDA
    area_km REAL NOT NULL,

    -- RELAÇÕES
    FOREIGN KEY (id_tempo) REFERENCES DimTempo (id_tempo),
    FOREIGN KEY (id_localidade) REFERENCES DimLocalidade (id_localidade),
    FOREIGN KEY (id_tipo) REFERENCES DimTipoDegradacao (id_tipo)
);