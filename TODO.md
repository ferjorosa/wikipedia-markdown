## Notebooks

* Agrupar los notebooks, dividirlos en directorios por casos de uso

## Revisar dato

* Buscar textos con .jpg

SELECT *
FROM articles
WHERE llm_cleaned_text LIKE '%.jpg%';

* Buscar aquellos que no solo tengan un 10% menos sino que tengan bastantes tokens mas. Por ejemplo Cosmology

## Buscar caracteres mal generados, se pueden arreglar (parace que es algo poco comun)

id: 8724 -> Kelvin

* Tiene varios

## Eliminar las listas no-numeradas de Wikipedia, que el parse lo pillo mal

;Lista

* as
* bbb
* ...

Podemos buscar que las lineas que empiecen por ";" se les elimine ese simbolo

## Revisar que las listas estan bien formateadas

He visto varios ejemplos donde empieza con "*" pero no le sigue un espacio, por lo que la lista queda mal

## Hay lineas con plots que son un lio, deberia eliminarlo en post-process

Por ejemplo, el articulo "Aurillac" https://simple.wikipedia.org/wiki/Aurillac

## Revisar todos los tags de mediawiki y quitar/dejar los que tenga sentido

Para ellos usamos BS4 y quitamos el contenido interior

https://www.mediawiki.org/wiki/Parser_extension_tags
