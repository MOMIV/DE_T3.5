# DE_T3.5
Задание 3.5

У вас получился прекрасно работающий dag, однако его еще можно улучшить! Для этого необходимо сделать следующие шаги:

Вынести параметры для вызова API в airflow variables.

Зачем? Подобные конфигурационные параметры должны иметь возможность изменяться без погружения в сам код.

Создайте новую переменную в интерфейсе Airflow, перенесите в нее значение url(Exchangerate.host - Unlimited & Free foreign, crypto exchange rates with currency conversion & EU VAT rates API) и названия валют, а затем импортируйте в код вашего dag’a.

Вынести подключение к БД в airflow connections.

Зачем? Данные для подключения небезопасно хранить в коде, и они также должны быть доступны всем разработчикам, работающим с airflow.

Также через интерфейс airflow создайте новое подключение к PostgresDB и перенесите туда все данные.

Обратите внимание на host — данный host соответствует внутреннему адресу контейнера с базой внутри docker-compose.

Протестируйте ваше соединение в рамках интерфейса airflow перед использованием. Как только соединение будет успешно установлено, импортируйте подключение в ваш airflow dag.

Решение

![Image alt](https://github.com/MOMIV/DE_T3.5/raw/main/pic/1.png)

![Image alt](https://github.com/MOMIV/DE_T3.5/raw/main/pic/2.png)

![Image alt](https://github.com/MOMIV/DE_T3.5/raw/main/pic/3.png)

![Image alt](https://github.com/MOMIV/DE_T3.5/raw/main/pic/4.png)