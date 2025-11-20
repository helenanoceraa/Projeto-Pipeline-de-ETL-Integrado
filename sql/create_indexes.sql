-- ÍNDICES

CREATE INDEX IF NOT EXISTS idx_fato_tempo ON FatoDesmatamento (id_tempo);
CREATE INDEX IF NOT EXISTS idx_fato_localidade ON FatoDesmatamento (id_localidade);
CREATE INDEX IF NOT EXISTS idx_fato_tipo ON FatoDesmatamento (id_tipo);
CREATE INDEX IF NOT EXISTS idx_dimtempo_ano ON DimTempo (ano);
CREATE INDEX IF NOT EXISTS idx_dimlocalidade_estado ON DimLocalidade (estado);
CREATE INDEX IF NOT EXISTS idx_dimtipo_tipo ON DimTipoDegradacao (tipo_degradacao);