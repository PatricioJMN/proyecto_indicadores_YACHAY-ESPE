
-- Crear la Base de Datos en ClickHouse
CREATE DATABASE IF NOT EXISTS indicadores;
USE indicadores;

-- Tabla para ENEMDU Persona
CREATE TABLE IF NOT EXISTS enemdu_persona (
    -- Variables de condición de actividad y empleo
    condact        Nullable(Int32),   -- Código de condición de actividad (1–8 según ENEMDU)
    desempleo      Nullable(Int32),   -- Indicador preprocesado de desempleo
    empleo         Nullable(Int32),   -- Indicador preprocesado de empleo 
    estrato        Nullable(Int32),   -- Nivel socioeconómico
    nnivins        Nullable(Int32),   -- Nivel de instrucción
    secemp         Nullable(Int32),   -- Sector de empleo (1=formal, 2=informal, 3=otro)
    rama1          Nullable(Int32),   -- Rama de actividad principal (p.ej. manufactura)
    upm            Nullable(Int128),  -- Unidad primaria de muestreo
    vivienda       Nullable(Int32),   -- Identificador de vivienda dentro de UPM
    
    -- Pesos y montos de ingreso
    fexp           Nullable(Float64), -- Factor de expansión para ponderar resultados
    ingpc          Nullable(Float64), -- Ingreso per cápita (tras procesamiento)
    ingrl          Nullable(Float64), -- Ingreso laboral reportado

    -- Identificadores únicos y relacionales
    grupo1         Nullable(Int32),
    hogar          Nullable(Int64),
    id_hogar       Nullable(Int128),
    id_persona     Nullable(Int128),
    id_vivienda    Nullable(Int128),

    -- Localización
    area           Nullable(String),
    ciudad         String,
    cod_inf        Nullable(String),
    panelm         Nullable(String),

    -- Resto de preguntas (32-bit signed)
    p01            Nullable(Int32),
    p02            Nullable(Int32),
    p03            Nullable(Int32),
    p04            Nullable(Int32),
    p06            Nullable(Int32),
    p07            Nullable(Int32),
    p09            Nullable(Int32),
    p10a           Nullable(Int32),
    p10b           Nullable(Int32),
    p15            Nullable(Int32),
    p20            Nullable(Int32),
    p21            Nullable(Int32),
    p22            Nullable(Int32),
    p23            Nullable(Int32),
    p24            Nullable(Int32),
    p25            Nullable(Int32),
    p26            Nullable(Int32),
    p27            Nullable(Int32),
    p28            Nullable(Int32),
    p29            Nullable(Int32),
    p32            Nullable(Int32),
    p33            Nullable(Int32),
    p34            Nullable(Int32),
    p35            Nullable(Int32),
    p36            Nullable(Int32),
    p37            Nullable(Int32),
    p38            Nullable(Int32),
    p39            Nullable(Int32),
    p40            Nullable(Int32),
    p41            Nullable(Int32),
    p42            Nullable(Int32),
    p44f           Nullable(Int32),
    p46            Nullable(Int32),
    p47a           Nullable(Int32),
    p47b           Nullable(Int32),
    p49            Nullable(Int32),
    p50            Nullable(Int32),
    p51a           Nullable(Int32),
    p51b           Nullable(Int32),
    p51c           Nullable(Int32),
    p63            Nullable(Int32),
    p64a           Nullable(Int32),
    p64b           Nullable(Int32),
    p65            Nullable(Int32),
    p66            Nullable(Int32),
    p67            Nullable(Int32),
    p68a           Nullable(Int32),
    p68b           Nullable(Int32),
    p69            Nullable(Int32),
    p70a           Nullable(Int32),
    p70b           Nullable(Int32),
    p71a           Nullable(Int32),
    p71b           Nullable(Int32),
    p72a           Nullable(Int32),
    p72b           Nullable(Int32),
    p73a           Nullable(Int32),
    p73b           Nullable(Int32),
    p74a           Nullable(Int32),
    p74b           Nullable(Int32),
    p75            Nullable(Int32),
    p76            Nullable(Int32),

    periodo        String -- Formato 'YYYYMM', usado como clave de partición y ordenamiento
)
ENGINE = MergeTree
ORDER BY (periodo)
SETTINGS index_granularity = 8192;

-- Tabla para ENEMDU Vivienda
CREATE TABLE enemdu_vivienda (
    -- Columnas de texto
    area            Nullable(String),
    ciudad			Nullable(String),
    conglomerado	Nullable(String),
    estrato			Nullable(String),
	
    -- Columnas flotantes de encuesta
    fexp            Nullable(Float64),
	
    -- IDs y contadores (64/128-bit signed)
    hogar			Nullable(Int64),
    id_hogar		Nullable(Int128),
    id_vivienda		Nullable(Int128),
	
    -- Columnas numéricas pequeñas
    panelm			Nullable(String),
    periodo			String,
    sector			Nullable(Int32),
    upm				Nullable(Int128),
	
    -- Resto de preguntas (32-bit signed)
    vi01            Nullable(Int32),
    vi02            Nullable(Int32),
    vi03a			Nullable(Int32),
    vi03b			Nullable(Int32),
    vi04a			Nullable(Int32),
    vi04b			Nullable(Int32),
    vi05a			Nullable(Int32),
    vi05b			Nullable(Int32),
    vi06            Nullable(Int32),
    vi07            Nullable(Int32),
    vi07a			Nullable(Int32),
    vi07b			Nullable(Int32),
    vi08            Nullable(Int32),
    vi09            Nullable(Int32),
    vi09a			Nullable(Int32),
    vi09b			Nullable(Int32),
    vi10            Nullable(Int32),
    vi101			Nullable(Int32),
    vi102			Nullable(Int32),
    vi10a			Nullable(Int32),
    vi11            Nullable(Int32),
    vi12            Nullable(Int32),
    vi13            Nullable(Int32),
    vi14			Nullable(Int32),
    vi141			Nullable(Int32),
    vi142			Nullable(Int32),
    vi143			Nullable(Int32),
    vi144			Nullable(Int32),
    vi1511			Nullable(Int32),
    vi1512			Nullable(Int32),
    vi1521			Nullable(Int32),
    vi1522			Nullable(Int32),
    vi1531			Nullable(Int32),
    vi1532			Nullable(Int32),
    vi1533			Nullable(Int32),
    vi1534			Nullable(Int32),
    vi1541			Nullable(Int32),
    vi1542			Nullable(Int32),
    vi1543			Nullable(Int32),
    vi1544			Nullable(Int32),
    vi1551			Nullable(Int32),
    vi1552			Nullable(Int32),
    vi1553			Nullable(Int32),
    vi1554			Nullable(Int32),
    vi1561			Nullable(Int32),
    vi1562			Nullable(Int32),
    vi1563			Nullable(Int32),
    vi1564			Nullable(Int32),
    vi16            Nullable(Int32),
    vi161			Nullable(Int32),
    vi162			Nullable(Int32),
    vi163			Nullable(Int32),
    vi164			Nullable(Int32),
    vi165			Nullable(Int32),
    vi166			Nullable(Int32),
    vi167			Nullable(Int32),
    vi168			Nullable(Int32),
    vi169			Nullable(Int32),
    vi1610			Nullable(Int32),
    vi1611			Nullable(Int32),
    vi1612			Nullable(Int32),
    vi1613			Nullable(Int32),
    vi1614			Nullable(Int32),
    vi17            Nullable(Int32),
    vi171			Nullable(Int32),
    vi172			Nullable(Int32),
    vi173			Nullable(Int32),
    vi174			Nullable(Int32),
    vi175			Nullable(Int32),
    vi176			Nullable(Int32),
    vi177			Nullable(Int32),
    vi178			Nullable(Int32),
    vi179			Nullable(Int32),
    vi1710			Nullable(Int32),
    vi1711			Nullable(Int32),
    vi1712			Nullable(Int32),
    vi1713			Nullable(Int32),
    vi1714			Nullable(Int32),
    vi18			Nullable(Int32),
    vi181			Nullable(Int32),
    vi182			Nullable(Int32),
    vi183			Nullable(Int32),
    vi184			Nullable(Int32),
    vi185			Nullable(Int32),
    vi186			Nullable(Int32),
    vi187			Nullable(Int32),
    vi188			Nullable(Int32),
    vi189			Nullable(Int32),
    vi1810			Nullable(Int32),
    vi1811			Nullable(Int32),
    vi1812			Nullable(Int32),
    vi1813			Nullable(Int32),
    vi1814			Nullable(Int32)
) ENGINE = MergeTree()
ORDER BY (periodo);

-- Tabla para los códigos de provincias/cantones
CREATE TABLE IF NOT EXISTS diccionario_provincias (
    CodigoProvincia   String,
    CodigoCanton      String,
    CodigoParroquia   String,
    NombreProvincia   String,
    NombreCanton      String,
    NombreParroquia   String
)
ENGINE = MergeTree()
ORDER BY (CodigoProvincia, CodigoCanton, CodigoParroquia);

-- Tabla para indicadores nacionales
drop table if exists indicadores_nacionales;
CREATE TABLE indicadores_nacionales
(
    anio          UInt16,
    periodo_num   UInt8,
    area          UInt8,
    tpg           Float32,
    tpb           Float32,
    td            Float32,
    empleo_total  Float32,
    formal        Float32,
    informal      Float32,
    adecuado      Float32,
    subempleo     Float32,
    no_remunerado Float32,
    otro_no_pleno Float32,
    brecha_adecuado_hm Float32,
    brecha_salarial_hm Float32,
    nini          Float32,
    desempleo_juvenil Float32,
    trabajo_infantil Float32,
    manufactura_empleo Float32
)
ENGINE = MergeTree
ORDER BY (anio, periodo_num, area);

-- Tabla para indicadores por cantón y parroquia
drop table if exists indicadores_canton;
CREATE TABLE indicadores_canton
(
    CodigoProvincia   String,
    NombreProvincia   String,
    CodigoCanton      String,
    NombreCanton      String,
    CodigoParroquia   String,
    NombreParroquia   String,
    anio               UInt16,
    periodo_num        UInt8,
    area               UInt8,
    tpg                Float32,
    tpb                Float32,
    td                 Float32,
    empleo_total       Float32,
    formal             Float32,
    informal           Float32,
    adecuado           Float32,
    subempleo          Float32,
    no_remunerado      Float32,
    otro_no_pleno      Float32,
    brecha_adecuado_hm Float32,
    brecha_salarial_hm Float32,
    nini               Float32,
    desempleo_juvenil  Float32,
    trabajo_infantil   Float32,
    manufactura_empleo Float32
)
ENGINE = MergeTree
ORDER BY (CodigoProvincia, CodigoCanton, CodigoParroquia, anio, periodo_num);

-- Vista materializada: indicadores nacionales
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_indicadores_nacionales
TO indicadores_nacionales
AS
SELECT
    toUInt16(substring(periodo,1,4)) AS anio,
    toUInt8(substring(periodo,5,2)) AS periodo_num,
    toUInt8(area) AS area,
    ifNull(100.0 * sumIf(fexp, p03 >= 15 AND condact BETWEEN 1 AND 8) / sumIf(fexp, p03 >= 15), 0) AS tpg,
    ifNull(100.0 * sumIf(fexp, condact BETWEEN 1 AND 8) / sumIf(fexp,1), 0) AS tpb,
    ifNull(100.0 * sumIf(fexp, condact IN (7,8)) / sumIf(fexp, condact BETWEEN 1 AND 8), 0) AS td,
    ifNull(100.0 * sumIf(fexp, condact BETWEEN 1 AND 6) / sumIf(fexp, condact BETWEEN 1 AND 8), 0) AS empleo_total,
    ifNull(100.0 * sumIf(fexp, secemp = 1) / sumIf(fexp, condact BETWEEN 1 AND 6), 0) AS formal,
    ifNull(100.0 * sumIf(fexp, secemp = 2) / sumIf(fexp, condact BETWEEN 1 AND 6), 0) AS informal,
    ifNull(100.0 * sumIf(fexp, condact = 1) / sumIf(fexp, condact BETWEEN 1 AND 8), 0) AS adecuado,
    ifNull(100.0 * sumIf(fexp, condact IN (2,3)) / sumIf(fexp, condact BETWEEN 1 AND 8), 0) AS subempleo,
    ifNull(100.0 * sumIf(fexp, condact = 5) / sumIf(fexp, condact BETWEEN 1 AND 8), 0) AS no_remunerado,
    ifNull(100.0 * sumIf(fexp, condact = 4) / sumIf(fexp, condact BETWEEN 1 AND 8), 0) AS otro_no_pleno,
    ifNull((100.0 * sumIf(fexp, p02 = 1 AND condact = 1) / sumIf(fexp, p02 = 1 AND condact BETWEEN 1 AND 8)
       - 100.0 * sumIf(fexp, p02 = 2 AND condact = 1) / sumIf(fexp, p02 = 2 AND condact BETWEEN 1 AND 8)), 0) AS brecha_adecuado_hm,
    ifNull(((sumIf(fexp * ingrl, p02 = 1 AND ingrl > 0) / sumIf(fexp, p02 = 1 AND ingrl > 0))
      - (sumIf(fexp * ingrl, p02 = 2 AND ingrl > 0) / sumIf(fexp, p02 = 2 AND ingrl > 0)))
      / (sumIf(fexp * ingrl, p02 = 1 AND ingrl > 0) / sumIf(fexp, p02 = 1 AND ingrl > 0)) * 100.0, 0) AS brecha_salarial_hm,
    ifNull(100.0 * sumIf(fexp, p03 BETWEEN 15 AND 24 AND (p07 = 2 OR p07 IS NULL)) / sumIf(fexp, p03 BETWEEN 15 AND 24), 0) AS nini,
    ifNull(100.0 * sumIf(fexp, p03 BETWEEN 18 AND 29 AND condact IN (7,8)) / sumIf(fexp, p03 BETWEEN 18 AND 29 AND condact BETWEEN 1 AND 8), 0) AS desempleo_juvenil,
    ifNull(100.0 * sumIf(fexp, p03 BETWEEN 5 AND 14 AND (condact BETWEEN 1 AND 6 OR p24 > 0)) / sumIf(fexp, p03 BETWEEN 5 AND 14), 0) AS trabajo_infantil,
    ifNull(100.0 * sumIf(fexp, rama1 = 3) / sumIf(fexp, condact BETWEEN 1 AND 6), 0) AS manufactura_empleo
FROM enemdu_persona
GROUP BY anio, periodo_num, area;

-- Vista materializada: indicadores por cantón y parroquia
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_indicadores_canton
TO indicadores_canton
AS
SELECT
    substring(ciudad,1,2) AS CodigoProvincia,
    dic.NombreProvincia,
    substring(ciudad,3,2) AS CodigoCanton,
    dic.NombreCanton,
    substring(ciudad,5,2) AS CodigoParroquia,
    dic.NombreParroquia,
    toUInt16(substring(periodo,1,4)) AS anio,
    toUInt8(substring(periodo,5,2)) AS periodo_num,
    toUInt8(area) AS area,
    ifNull(100.0 * sumIf(fexp, p03 >= 15 AND condact BETWEEN 1 AND 8) / sumIf(fexp, p03 >= 15), 0) AS tpg,
    ifNull(100.0 * sumIf(fexp, condact BETWEEN 1 AND 8) / sumIf(fexp,1), 0) AS tpb,
    ifNull(100.0 * sumIf(fexp, condact IN (7,8)) / sumIf(fexp, condact BETWEEN 1 AND 8), 0) AS td,
    ifNull(100.0 * sumIf(fexp, condact BETWEEN 1 AND 6) / sumIf(fexp, condact BETWEEN 1 AND 8), 0) AS empleo_total,
    ifNull(100.0 * sumIf(fexp, secemp = 1) / sumIf(fexp, condact BETWEEN 1 AND 6), 0) AS formal,
    ifNull(100.0 * sumIf(fexp, secemp = 2) / sumIf(fexp, condact BETWEEN 1 AND 6), 0) AS informal,
    ifNull(100.0 * sumIf(fexp, condact = 1) / sumIf(fexp, condact BETWEEN 1 AND 8), 0) AS adecuado,
    ifNull(100.0 * sumIf(fexp, condact IN (2,3)) / sumIf(fexp, condact BETWEEN 1 AND 8), 0) AS subempleo,
    ifNull(100.0 * sumIf(fexp, condact = 5) / sumIf(fexp, condact BETWEEN 1 AND 8), 0) AS no_remunerado,
    ifNull(100.0 * sumIf(fexp, condact = 4) / sumIf(fexp, condact BETWEEN 1 AND 8), 0) AS otro_no_pleno,
    ifNull((100.0 * sumIf(fexp, p02 = 1 AND condact = 1) / sumIf(fexp, p02 = 1 AND condact BETWEEN 1 AND 8)
       - 100.0 * sumIf(fexp, p02 = 2 AND condact = 1) / sumIf(fexp, p02 = 2 AND condact BETWEEN 1 AND 8)), 0) AS brecha_adecuado_hm,
    ifNull(((sumIf(fexp * ingrl, p02 = 1 AND ingrl > 0) / sumIf(fexp, p02 = 1 AND ingrl > 0))
      - (sumIf(fexp * ingrl, p02 = 2 AND ingrl > 0) / sumIf(fexp, p02 = 2 AND ingrl > 0)))
      / (sumIf(fexp * ingrl, p02 = 1 AND ingrl > 0) / sumIf(fexp, p02 = 1 AND ingrl > 0)) * 100.0, 0) AS brecha_salarial_hm,
    ifNull(100.0 * sumIf(fexp, p03 BETWEEN 15 AND 24 AND (p07 = 2 OR p07 IS NULL)) / sumIf(fexp, p03 BETWEEN 15 AND 24), 0) AS nini,
    ifNull(100.0 * sumIf(fexp, p03 BETWEEN 18 AND 29 AND condact IN (7,8)) / sumIf(fexp, p03 BETWEEN 18 AND 29 AND condact BETWEEN 1 AND 8), 0) AS desempleo_juvenil,
    ifNull(100.0 * sumIf(fexp, p03 BETWEEN 5 AND 14 AND (condact BETWEEN 1 AND 6 OR p24 > 0)) / sumIf(fexp, p03 BETWEEN 5 AND 14), 0) AS trabajo_infantil,
    ifNull(100.0 * sumIf(fexp, rama1 = 3) / sumIf(fexp, condact BETWEEN 1 AND 6), 0) AS manufactura_empleo
FROM enemdu_persona
LEFT JOIN diccionario_provincias AS dic
  ON dic.CodigoProvincia = substring(ciudad,1,2)
 AND dic.CodigoCanton    = substring(ciudad,3,2)
 AND dic.CodigoParroquia = substring(ciudad,5,2)
GROUP BY CodigoProvincia, NombreProvincia,
         CodigoCanton, NombreCanton,
         CodigoParroquia, NombreParroquia,
         anio, periodo_num, area;