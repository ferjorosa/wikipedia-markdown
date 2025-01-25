## Notebooks

* Agrupar los notebooks, dividirlos en directorios por casos de uso

## Revisar dato

* Buscar textos con .jpg

SELECT *
FROM articles
WHERE llm_cleaned_text LIKE '%.jpg%';

* Buscar aquellos que no solo tengan un 10% menos sino que tengan bastantes tokens mas. Por ejemplo Cosmology
