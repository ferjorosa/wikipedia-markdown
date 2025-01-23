Hay veces que el modelo va mal y se le va la olla. En tal caso, tengo que mirar aquellas filas que tengan menos 
de un 90% de los tokens originales, ya que hay algo raro

---

He tenido que añadir lo de Gallery al prompt ya que no quitaba las imagenes bien. Es por eso que tuve que buscar con

SELECT *
FROM articles
WHERE llm_cleaned_text LIKE '%.jpg%';

----

Ciertos simbolos UTF no los ha añadido correctamente, como los de la tabla de Armenia

----

Se atasca cada X y no parece que vaya bien lo del timeout? (lo he observado en forma de notebook tambien)

----

Hay veces que el modelo te añade ```markdown``` al principio. Es por ello que hemos revisado manualmente todos los casos

----

Si bien habiamos hecho una version inicial del prompt, lo cierto es que nos ibamos encontrando mas y mas cosas segun avanzabamos,
asi que fuimos añadiendo puntos en el prompt y revisando manualmente los textos para encontrar patrones cons los que filtrar