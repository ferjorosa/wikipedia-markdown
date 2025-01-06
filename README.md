# wikipedia-markdown

* El dataset de Huggingface esta anticuado
* Usar mwparserfromhell quita demasiada estructura del texto (listas, tablas, etc.)
* La API de Wikipedia (https://github.com/martin-majlis/Wikipedia-API) devuelve texto similar al de los dumps, tenemos el mismo problema de esturctura

Una idea que podria estar bien es parsear la web de Wikipedia con BS4 y extraer el texto puro junto a las tablas (similar a los dumps)
* Es una opcion pero llevaria tiempo y especializacion sobre Wikipedia, habraiq que entender sus clases y hacer ingenieria inversa. Puede que incluso con eso hubiera que terminar usando LLMs

En el futuro, podemos utilizar este dato como base para finetunear otros modelos mas pequeños y ver si funciona

Quizas otros prompts son mejores. Tengo la intuicion de que segun avancen los modelos, procesar este tipo de datos sera aun mas faci....

-------

Una vez todo funcione, tengo que hacer una funcione que vaya almacenando los textos en una base de datos SQLite,
ya que esta probablemente permite que haga inserciones paralelas. La otra opciones guardarlo como archivos
individuales en disco.

Lo primero es checkear que dicho articulo no este ya en la DB (vamos en orden). En caso de no
estar, procesamos el articulo y lo guardamos en la DB,

Columnas:
* article_id
* article_title
* article_text_raw
* article_text_markdown
* model (HuggingFace)

Podemos hacer una primera prueba con 1 articulo en forma secuencial,
luego con 10 articulos de forma paralela,
luego con 100

Una vez todo va bien, ponemos la maquina a funcionar con X threads


-------------------

- Añadir un Project_paths file / enum para no tener que hacer "../" en los notebooks
- Documentacion (obviamente)
