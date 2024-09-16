# tfm_public
Autor: Antonio Peña Martínez
Email: antpema@gmail.com
TFM Uned Master Big Data 2023-2024

El fichero padre del TFM es el bets.ipynb, situado en la raíz del proyecto

Para validar la demo se puede utilizar la siguiente URL desde cualquier navegador:

https://europe-southwest1-prefab-breaker-433816-j4.cloudfunctions.net/football-result-predict?day=yyyy-mm-dd&team=name

Ejemplo de equipos: "Real Madrid" y "Barcelona"

NOTA 1: Tener en consideración que la cuenta donde está desplegada la API es gratuita con recursos limitados de ahí que se recomiende su invocación especificando tanto día como equipo. Aún así, la API puede tardar varios segundos / minutos pero si se utiliza, por ejemplo, el navegador de Microsoft Edge, no hay problema. Otros navegadores podrían dar timeout.

NOTA 2: Sólo hay predicciones para partidos de un equipo, como mucho, para una semana antes (si se desea comprobar resultados ya conocidos) como para una semana después (si se desea obtener una predicción de un partido que aún no ha sucedido)

NOTA 3: Lógicamente, se pueden hacer varias invocaciones a la API pero por temas de rendimiento es mejor realizarlas secuencialmente y los resultados obtenidos pueden variar ya que el valor de las apuestas también van variando.
