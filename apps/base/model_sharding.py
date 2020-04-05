import calendar
import math
from collections import OrderedDict
from importlib import import_module

from django.conf import settings
from django.contrib import admin
from django.core.management import commands
from django.db import connection
from django.forms import model_to_dict
from django.utils import timezone

SHARDING_COUNT_DEFAULT = getattr(settings, 'SHARDING_COUNT_DEFAULT', 10)
SHARDING_DATE_START_DEFAULT = getattr(settings, 'SHARDING_DATE_START_DEFAULT', '2020-01-01')
SHARDING_DATE_FORMAT_DEFAULT = getattr(settings, 'SHARDING_DATE_FORMAT_DEFAULT', '%Y%m')

shard_tables = {}
admin_opts_map = {}


def get_next_year_and_month(date):
    if date.month == 12:
        return date.year + 1, 1

    return date.year, date.month + 1


def create_model(abstract_model_class, sharding, meta_options=None):
    """Create sharding model which inherit from `abstract_model_class`."""

    model_name = abstract_model_class.__name__ + sharding
    table_name = "%s_%s%s" % (abstract_model_class._meta.app_label, abstract_model_class._meta.db_table, sharding)

    class Meta:
        db_table = table_name

    if meta_options is None:
        meta_options = {}

    for k, v in abstract_model_class.Meta.__dict__.items():
        if k.startswith('__') or k in ('abstract', 'db_table'):
            continue

        setattr(Meta, k, v)

    meta_options.update(abstract_model_class.default_meta_options(sharding))
    for k, v in meta_options.items():
        setattr(Meta, k, v)

    attrs = {
        '__module__': abstract_model_class.__module__,
        'Meta': Meta,
    }

    ModelClass = type(model_name, (abstract_model_class,), attrs)
    shard_tables[table_name] = ModelClass

    class Admin(admin.ModelAdmin):
        pass

    label_lower = abstract_model_class._meta.label_lower
    if admin_opts_map.get(label_lower):
        for key, value in admin_opts_map[label_lower].items():
            setattr(Admin, key, value)
        admin.site.register(ModelClass, Admin)


def register_admin_opts(app_config_name, opts):
    if app_config_name in admin_opts_map:
        admin_opts_map[app_config_name].update(opts)
    else:
        admin_opts_map[app_config_name] = opts


def exec_command(command, app_label):
    """Execute django command even the django app still running."""

    cmd_module_name = '%s.%s' % (commands.__name__, command)
    cmd_module = import_module(cmd_module_name)
    cmd = getattr(cmd_module, 'Command')()
    parser = cmd.create_parser('./manage.py', cmd)
    options = parser.parse_args([app_label])
    cmd_options = vars(options)
    args = cmd_options.pop('args', ())
    cmd.execute(*args, **cmd_options)


class ShardingMixin(object):
    @classmethod
    def shard(cls, sharding_source=None):
        sharding = cls.get_sharding(str(sharding_source))
        db_table = "%s_%s%s" % (cls._meta.app_label, cls._meta.db_table, sharding)
        if db_table not in shard_tables:
            create_model(cls, sharding)

            cursor = connection.cursor()
            tables = [table_info.name for table_info in connection.introspection.get_table_list(cursor)]
            if db_table not in tables:
                for cmd in ('makemigrations', 'migrate'):
                    exec_command(cmd, cls._meta.app_label)

        return shard_tables[db_table]

    @classmethod
    def get_sharding(cls, sharding_source=None):
        sharding_list = cls.get_sharding_list()
        if sharding_source not in sharding_list:
            return cls.default_sharding()

        if getattr(cls, 'SHARDING_TYPE', 'date') == 'date':
            return sharding_source

        return str(int(sharding_source) % int(getattr(cls, 'SHARDING_COUNT', SHARDING_COUNT_DEFAULT)))

    @classmethod
    def get_sharding_list(cls):
        if getattr(cls, 'SHARDING_TYPE', 'date') == 'date':
            return cls.get_date_sharding_list()

        sharding_count = int(getattr(cls, 'SHARDING_COUNT', SHARDING_COUNT_DEFAULT))
        return (str(sharding) for sharding in range(sharding_count))

    @classmethod
    def get_date_sharding_list(cls):
        """
        Generate a date sharding sequence of year or month or day, which starts from date setting named
        `SHARDING_DATE_START` to ends of current date.
        """

        date_start = getattr(cls, 'SHARDING_DATE_START', SHARDING_DATE_START_DEFAULT)
        date_end = timezone.now().date()
        date_sharding_format = getattr(cls, 'SHARDING_DATE_FORMAT', SHARDING_DATE_FORMAT_DEFAULT)

        if isinstance(date_start, str):
            date_start = timezone.datetime.strptime(date_start, '%Y-%m-%d').date()

        while date_start <= date_end:
            if date_sharding_format.endswith('%Y'):
                yield date_start.strftime(date_sharding_format)
                date_start = date_start.replace(year=date_start.year + 1, day=1)
            elif date_sharding_format.endswith('%d'):
                _, month_length = calendar.monthrange(date_start.year, date_start.month)
                if date_start.day <= month_length:
                    yield date_start.strftime(date_sharding_format)
                    date_start = date_start.replace(day=date_start.day + 1)

                if date_start.day == month_length:
                    yield date_start.strftime(date_sharding_format)
                    next_year, next_month = get_next_year_and_month(date_start)
                    date_start = date_start.replace(year=next_year, month=next_month, day=1)
            else:
                yield date_start.strftime('%Y%m')
                next_year, next_month = get_next_year_and_month(date_start)
                date_start = date_start.replace(year=next_year, month=next_month, day=1)

    @classmethod
    def default_sharding(cls):
        if getattr(cls, 'SHARDING_TYPE', 'date') == 'date':
            date_sharding_format = getattr(cls, 'SHARDING_DATE_FORMAT', SHARDING_DATE_FORMAT_DEFAULT)
            return timezone.now().strftime(date_sharding_format)

        return '0'

    @classmethod
    def default_meta_options(cls, sharding):
        return {
            'verbose_name': cls.__name__ + sharding,
            'verbose_name_plural': cls.__name__ + sharding
        }

    @classmethod
    def paginate_sharding(cls, page, page_size):
        """Paginate the querysets of all shardings."""

        total_count = 0
        sharding_count_map = OrderedDict()
        for sharding in cls.get_sharding_list():
            count = cls.shard(sharding).objects.count()
            sharding_count_map[sharding] = count
            total_count += count

        max_page = math.ceil(total_count / page_size) or 1
        if page > max_page:
            page = max_page

        prev_page = 1
        diff = 0
        accumulation_count = 0
        results = []
        for sharding, count in sharding_count_map.items():
            accumulation_count += count
            page_num = math.ceil(accumulation_count / page_size)
            if prev_page <= page <= page_num:
                if diff:
                    qs = cls.shard(sharding).objects.all()[0:diff]
                else:
                    start = count - (accumulation_count - (page - 1) * page_size)
                    end = start + page_size
                    qs = cls.shard(sharding).objects.all()[start:end]

                for obj in qs:
                    results.append(model_to_dict(obj))
                diff = page_size - len(results)
                if diff:
                    continue

                break

            prev_page = page_num

        ret = {
            'result': results,
            'count': total_count,
            'next_page': page + 1 if page < max_page else -1
        }
        return ret
