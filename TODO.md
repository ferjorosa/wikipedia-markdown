* Terminar lo de las tablas y probar todos los casos posibles
* Agrupar los notebooks, dividirlos en directorios por casos de uso
* Añadir lo de replace_tables en la parte de procesamiento
  * Utilizar mwparserfromhell?
    * El que tenga mas estrellas o sea mas facil/mejor de usar y este mas seguido...
    * Tendria que pasar el metodo a wikitextparser sino...


Mirar si con regex soy capaz de eliminar correctamente

Testear con una GPU o CPU cual es la velocidad de token/s cuando paso un batch de 1 versus cuando paso un batch
grande a un modelo pequeño. Si finetuneamos algo podriamos acabar con un modelo ultra-rapido.
* Quizas para articulos pequeños tiene sentido usar un modelo muy pequeño
* Para medianos uno mas grande
* Y para los grandes con tablas y demas, usar uno de los de pago.


* Buscar algun tipo de fuzzy matcher para el HTML tags (?)

* Revisar como podemos arreglar mediante regex los titulos

===
  Fer ===

* Añadir espacios "\n" entre titulos y parrafos y tablas y luego quitarlos para que quede homogenizado
* Pasar a markdwon lo que se pueda de forma deterministica (titulos?)
* LLM para limpiar el texto de cosas que no haya podido pillar los approaches deterministicos


La idea es limpiar lo mas posible el dato, luego seria revisado por un LLM

------

Guardar los articulos que se parsean en una DB en vez de parquet
(para evitar que si falla el proceso se pierda el trabajo y para un mejor storage de la info,
no vas a tener un parquet de 60GB)


-----

* Añadir espacios "\n" entre titulos y parrafos y tablas y luego quitarlos para que quede homogenizado
* Guardar los articulos que se parsean en una DB en vez de parquet (para evitar que si falla el proceso se pierda el trabajo y para un mejor storage de la info,  no vas a tener un parquet de 60GB)
* Revisar lo de divide by sections de forma que no  parta por la mitad. Habria que asegurarse que es un titulo (dejar una marca?)

-----

Hacer limpieza de codigo y en temas de DB quedarnos solo con las soluciones que inserten en batch y procesen con tqdm
