# Экспорт NWC

Скрипт для пакетного экспорта моделей Revit в формат Navisworks (.nwc) через pyRevit.

## Описание

Скрипт автоматизирует процесс экспорта множества RVT-файлов в формат NWC для использования в Navisworks. Основная цель - оптимизированный экспорт без связей, аннотаций и импортированных элементов.

## Основные возможности

- **Пакетный экспорт**: обработка нескольких RVT-файлов за один запуск
- **Оптимизированная загрузка**: открытие файлов без рабочих наборов связей
- **Автоматическое создание вида**: создание/использование специального 3D-вида "Navisworks"
- **Фильтрация элементов**: исключение связей, аннотаций, импортов, облаков точек
- **Отсоединенный режим**: открытие файлов в detached-режиме без синхронизации

## Зависимости

### Внутренние модули (lib/)
- `openbg.py` - модуль фонового открытия RVT-файлов с:
  - Гибкой настройкой рабочих наборов (предикаты, префиксы, имена)
  - Автоматическим подавлением предупреждений (`SuppressWarningsPreprocessor`)
  - Поддержкой detached-режима
- `closebg.py` - модуль безопасного закрытия документов

### Revit API
- `Autodesk.Revit.DB`: ModelPathUtils, View3D, NavisworksExportOptions, FilteredElementCollector
- Категории для скрытия: OST_RvtLinks, OST_ImportInstance, OST_Annotations и др.

### pyRevit
- `script`, `coreutils`, `forms` - для UI, таймеров и прогресс-бара

## Архитектура

### Логика открытия файлов

Используется модуль `openbg.py` с режимом `'predicate'` для фильтрации рабочих наборов:

```python
def workset_filter(ws_name):
    name = (ws_name or u"").strip()
    # Исключаем: начинающиеся с '00_'
    if name.startswith(u'00_'):
        return False
    # Исключаем: содержащие 'Link' или 'Связь'
    name_lower = name.lower()
    if u'link' in name_lower or u'связь' in name_lower:
        return False
    return True
```

**Не загружаются рабочие наборы:**
- Начинающиеся с `00_` (архивные)
- Содержащие `Link` (связи на английском)
- Содержащие `Связь` (связи на русском)

Это значительно ускоряет открытие файлов, т.к. не грузятся геометрия и данные связанных моделей.

### Подготовка вида для экспорта

Функция `find_or_create_navis_view()` создает изометрический 3D-вид "Navisworks" со следующими настройками:

**Скрытые категории:**
- `OST_RvtLinks`, `OST_LinkInstances` - связи Revit
- `OST_ImportInstance`, `OST_ImportsInFamilies` - импортированные DWG/DXF
- `OST_Cameras`, `OST_Views` - камеры и виды
- `OST_PointClouds`, `OST_PointCloudsHardware` - облака точек
- `OST_Levels`, `OST_Grids` - уровни и оси
- `OST_Annotations`, `OST_Dimensions`, `OST_TextNotes` - все аннотации
- Все категории типа `CategoryType.Annotation`

**Дополнительные настройки:**
- `view.AreImportCategoriesHidden = True` - скрыть импортированные категории
- `view.AreAnnotationCategoriesHidden = True` - скрыть аннотационные категории
- `view.IsSectionBoxActive = False` - отключить 3D-подрезку
- Отключение шаблона вида (чтобы настройки видимости не блокировались)

### Безопасная работа с BuiltInCategory

Скрипт совместим с Revit 2022/2023 благодаря безопасной проверке существования констант:

```python
def _resolve_bic(name):
    if not name:
        return None
    try:
        if Enum.IsDefined(BuiltInCategory, name):
            return Enum.Parse(BuiltInCategory, name)
    except Exception:
        pass
    return None
```

Если категория отсутствует в текущей версии Revit, она просто пропускается без ошибок.

### Процесс экспорта

1. **Выбор моделей**: через `forms.pick_file()` или кастомный `sup.select_file()`
2. **Выбор папки**: через `forms.pick_folder()` (без создания подпапок)
3. **Для каждой модели:**
   - Открытие в detached-режиме с фильтрацией рабочих наборов
   - Поиск/создание вида "Navisworks"
   - Скрытие служебных категорий
   - Регенерация документа
   - Экспорт через `doc.Export()` с `NavisworksExportScope.View`
   - Закрытие без синхронизации

## Оптимизации производительности

### 1. Фильтрация рабочих наборов
- **Проблема**: при `worksets='all'` загружаются все связи → медленное открытие
- **Решение**: предикат исключает РН со связями → быстрое открытие

### 2. Detached-режим
- **Опция**: `detach=True` → `DetachAndPreserveWorksets`
- **Эффект**: нет синхронизации с центральной моделью, быстрее открытие/закрытие

### 3. Автоматическое подавление предупреждений
- **Проблема**: диалоги Revit с предупреждениями блокируют выполнение скрипта
- **Решение**: `SuppressWarningsPreprocessor` автоматически обрабатывает предупреждения через `IFailuresPreprocessor`
- **Механизм**:
  - Подписка на событие `Application.FailuresProcessing` перед открытием
  - Автоматическое удаление всех Warning через `DeleteWarning()`
  - Попытка разрешить Error через `ResolveFailure()`
  - Отписка от события после открытия (через `finally`)
- **Параметр**: `suppress_warnings=True` (включен по умолчанию)

### 4. Экспорт только видимых элементов
- Скрытие связей/импортов → меньший размер NWC
- Скрытие аннотаций → чистая 3D-модель для Navisworks

### 5. Явное скрытие ImportInstance
Дополнительная защита через `view.HideElements()`:
```python
for ii in FilteredElementCollector(doc, view.Id).OfClass(ImportInstance):
    if view.CanElementBeHidden(ii.Id):
        ids.Add(ii.Id)
view.HideElements(ids)
```

## Вывод информации

Скрипт выводит для каждой модели:
- Путь к экспортируемому файлу
- **Информация об обработанных предупреждениях/ошибках** (если были):
  - Количество автоматически обработанных предупреждений и ошибок
  - Первые 5 предупреждений с полным текстом
  - Первые 3 ошибки с полным текстом
  - Суммарное количество, если их больше
- Статус импортированных категорий (`AreImportCategoriesHidden`)
- Количество `ImportInstance` в виде
- Количество видимых элементов
- Время открытия и экспорта
- Статус завершения (OK/Ошибка)

### Пример вывода с предупреждениями:
```
⚠️ При открытии обработано автоматически: 5 предупреждений, 0 ошибок
  1. Недостающие ссылки: Архитектура_Связь.rvt
  2. Недостающие ссылки: КР_Связь.rvt
  3. Удалены компоненты экземпляров группы
  4. Устаревшее семейство: Дверь-001
  5. Предупреждение о рабочих наборах
```

## Конфигурация

### Константы
```python
SAVE_CREATED_VIEW = False  # не сохранять созданный вид (т.к. detached-режим)
```

### Папка по умолчанию
```python
~/Documents/NWC_Export
```

## Технические детали

### SuppressWarningsPreprocessor (openbg.py)

Класс-обработчик, реализующий `IFailuresPreprocessor` для автоматической обработки диалогов Revit:

```python
class SuppressWarningsPreprocessor(IFailuresPreprocessor):
    def __init__(self):
        self.warnings = []  # Список текстов предупреждений
        self.errors = []    # Список текстов ошибок

    def PreprocessFailures(self, failuresAccessor):
        failures = failuresAccessor.GetFailureMessages()
        for failure in failures:
            severity = failure.GetSeverity()
            desc = failure.GetDescriptionText()  # Получаем текст предупреждения/ошибки

            # Удаляем предупреждения (Warning)
            if severity == FailureSeverity.Warning:
                self.warnings.append(desc)  # Сохраняем для отчета
                failuresAccessor.DeleteWarning(failure)
            # Для ошибок пытаемся использовать дефолтное решение
            elif severity == FailureSeverity.Error:
                self.errors.append(desc)  # Сохраняем для отчета
                failuresAccessor.ResolveFailure(failure)
        return FailureProcessingResult.Continue

    def get_summary(self):
        """Возвращает сводку для отчета"""
        return {
            'warnings': list(self.warnings),
            'errors': list(self.errors),
            'total_warnings': len(self.warnings),
            'total_errors': len(self.errors)
        }
```

**Жизненный цикл:**
1. Создается экземпляр перед `app.OpenDocumentFile()`
2. Подписывается на `app.FailuresProcessing` событие
3. При возникновении ошибок/предупреждений автоматически вызывается `PreprocessFailures()`
4. **Тексты всех предупреждений/ошибок сохраняются в списки**
5. После открытия файла отписывается через `finally` блок
6. Вызывается `get_summary()` для получения отчета

**Обрабатываемые случаи:**
- Недостающие связи ("0 Ошибок, 5 Предупреждений")
- Удаленные компоненты экземпляров групп
- Устаревшие семейства
- Предупреждения о рабочих наборах
- Другие Warning/Error с возможностью автоматического разрешения

## Совместимость

- **Revit**: 2022, 2023 (проверка существования BuiltInCategory через Enum)
- **pyRevit**: требуется установленный pyRevit с поддержкой Revit API

## Структура файлов

```
Экспорт NWC.pushbutton/
├── navis_export_script.py    # основной скрипт
├── bundle.yaml                # метаданные кнопки
├── icon.png                   # иконка кнопки
└── claude.md                  # этот файл
```

## Примечания

- Скрипт не выполняет `git commit` после экспорта
- NWC-файлы сохраняются в корневую папку (без подпапок под каждую модель)
- При ошибке открытия модель пропускается, экспорт продолжается
- Вид "Navisworks" не удаляется после экспорта (но и не сохраняется, т.к. detached)

## Реализованные возможности

- [x] Автоматическое подавление предупреждений Revit
- [x] Детальный отчет об обработанных предупреждениях/ошибках
- [x] Оптимизированная загрузка файлов (без связей)

## Возможные улучшения

- [ ] Добавить опцию выбора формата (NWC/NWD)
- [ ] Настройка списка скрываемых категорий через UI
- [ ] Поддержка экспорта по уровням/зонам
- [ ] Логирование в отдельный файл для архивирования
- [ ] Параллельный экспорт (если Revit API позволит)
- [ ] Экспорт сводки об ошибках в CSV/Excel
