CREATE TABLE public.models (
  "modelName"       TEXT PRIMARY KEY,
  "modelType"       TEXT NOT NULL,
  "modelParams"     TEXT NOT NULL,
  "isTrained"       BOOLEAN NOT NULL DEFAULT False,
  "trainAccuracy"   NUMERIC(21,20),
  "testAccuracy"    NUMERIC(21,20),
  "weights"         BYTEA,
  "modifyDate"      TIMESTAMP NOT NULL DEFAULT now()
);
