# wikipedia-markdown

* El dataset de Huggingface esta anticuado
* Usar mwparserfromhell quita demasiada estructura del texto (listas, tablas, etc.)
* La API de Wikipedia (https://github.com/martin-majlis/Wikipedia-API) devuelve texto similar al de los dumps, tenemos el mismo problema de esturctura

Una idea que podria estar bien es parsear la web de Wikipedia con BS4 y extraer el texto puro junto a las tablas (similar a los dumps)
* Es una opcion pero llevaria tiempo y especializacion sobre Wikipedia, habraiq que entender sus clases y hacer ingenieria inversa. Puede que incluso con eso hubiera que terminar usando LLMs

En el futuro, podemos utilizar este dato como base para finetunear otros modelos mas pequeños y ver si funciona

Quizas otros prompts son mejores. Tengo la intuicion de que segun avancen los modelos, procesar este tipo de datos sera aun mas faci....
