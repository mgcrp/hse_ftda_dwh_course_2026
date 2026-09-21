CREATE TABLE public.dataset (
  "fixed acidity"           NUMERIC(18,4),
  "volatile acidity"        NUMERIC(18,4),
  "citric acid"             NUMERIC(18,4),
  "residual sugar"          NUMERIC(18,4),
  "chlorides"               NUMERIC(18,4),
  "free sulfur dioxide"     NUMERIC(18,4),
  "total sulfur dioxide"    NUMERIC(18,4),
  "density"                 NUMERIC(18,4),
  "pH"                      NUMERIC(18,4),
  "sulphates"               NUMERIC(18,4),
  "alcohol"                 NUMERIC(18,4),
  "quality"                 INT
);

COPY public.dataset(
  "fixed acidity",
  "volatile acidity",
  "citric acid",
  "residual sugar",
  "chlorides","free sulfur dioxide",
  "total sulfur dioxide",
  "density",
  "pH",
  "sulphates",
  "alcohol",
  "quality"
) FROM '/var/lib/postgresql/data/data.csv' DELIMITER ';' CSV HEADER;
