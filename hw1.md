**Основная информация**
**Услуга**: проведение онлайн-платежей с банковских счетов граждан
**Проблема**: участились случаи мошенничества
**Задача**: внедрить в IT-инфраструктуру модуль на базе машинного обучения, способный в реальном времени оценивать, является ли проводимая транзакция мошеннической (уже существует своя IT-инфраструктура, требование - real-time)

**Параметры**:
**Финансирование**: 10 млн. руб.
**Сроки**: 3 месяца - прототип, 6 месяцев - готовый продукт.
**Требования к точности**:
<= 2/100 мошеннических транзакций, приводящих к потере денежных средств (значит, нужно максимизировать recall=TP/(TP+FN)>0.98 сколько относим к мошенническим из всех мошеннических, т.е. должно к мошенническим должно быть отнесено минимум 98 из каждых реальных 100 мошеннических).
Общий ущерб клиентов за месяц <= 500 тыс. руб.
Если система определит корректную транзакцию как мошенническую (false positive) , эта транзакция будет отклонена, а пользователь будет недоволен. Доля FP не должна превысить 5%, т.е. precision=TP/(TP+FP)>0.95

**Нагрузка на систему**:
средняя: 50 tr per sec
максимальная: 400 tr per sec

**Входные данные**:
csv-файлы, содержат всю информацию о транзакциях, включая данные клиента (конфиденциальная информация).


# 1. Сформулировать цели проектируемой антифрод-системы в соответствии с требованиями заказчика.
1. Оценка транзакций в режиме реального времени.
2. Обеспечение точности обнаружения мошеннических транзакций не ниже 98% 
3. Обеспечение не более 5% ложных срабатываний системы.
4. Обеспечение ущерба от пропущенных мошеннических транзакций не более 500 тыс. руб. в месяц.

# 2. Аргументированно выбрать метрику машинного обучения, оптимизация которой, на Ваш взгляд, позволит достичь поставленных целей.
Согласно требованиям к точности, система должна обеспечить <= 2/100 мошеннических транзакций, приводящих к потере денежных средств (**false negative**). Следовательно, ограничение на recall=TP/(TP+FN)>=0.98 - сколько относим к мошенническим из всех мошеннических, т.е. должно к мошенническим должно быть отнесено минимум 98 из каждых реальных 100 мошеннических).
Если система определит корректную транзакцию как мошенническую (**false positive**) , эта транзакция будет отклонена, а пользователь будет недоволен ,т.е. доля FP не должна превысить 5%, следовательно, precision=TP/(TP+FP)>=0.95.
Предлагаю максимизировать метрику **F1-score**, т.к. необходимо учитывать и **precision**, и **recall**.

# 3. Проанализировать особенности проекта с использованием, например, MISSION Canvas. Это позволит выделить специфику проекта и формализовать требования к нему, подобрать инструментарий и критерии оценки.
см. Machine Learning Canvas.pdf

# 4. Попытаться декомпозировать планируемую систему, определить ее основные функциональные части.

1. Хранилище для работы с данными (S3 / HDFS)
2. Вычислительный кластер (train / inference)
3. Model registry
4. Обработчик очереди транзакций на inference
5. Scheduler: загрузка новых транзакций и информации о мошенничестве из хранилища клиента в хранилище системы + обучение новой модели


# 5. Определить задачи, решение которых необходимо для достижения поставленных целей с учетом проведенного анализа. Задачи рекомендуется формулировать по принципу S.M.A.R.T. На текущий момент, пока не конкретизированы детали антифрод-системы, они могут быть представлены в достаточно общем виде. Но они позволят сформировать некоторый roadmap по реализации проекта.

1. Создать хранилище для работы с данными и перенести данные заказчика в хранилище
2. Создать вычислительный кластер
3. Создать Model registry
4. Создать пайплайн для обработки сырых данных и преобразования в датасет для модели
5. Настроить трэкинг артифактов моделей
6. Оптимизировать гиперпараметры моделей на валидационной выборке
7. Обучить финальную модель машинного обучения и разместить в Model registry
8. Создать Scheduler для подгрузки данных и переобучения моделей
9. Создать обработчик очереди транзакций на inference
10. Протестировать работу системы в режиме реального времени

# 6. Создать GitHub-репозиторий для проекта и разместить в нем сформулированные задачи (∼ 5 задач) на Kanban-доске в GitHub Projects со статусом «Новые».