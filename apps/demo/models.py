from django.db import models

from apps.base import model_sharding


class User(models.Model, model_sharding.ShardingMixin):
    user_name = models.CharField(max_length=50, unique=True)
    name = models.CharField(max_length=50)
    age = models.IntegerField(default=18)
    active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    # Constant-based sharding
    SHARDING_TYPE = 'precise'
    SHARDING_COUNT = 10

    def __str__(self):
        return "%s:%s" % (str(self.id), self.name)

    class Meta:
        abstract = True
        db_table = "user_"


def init_user_models():
    admin_opts = {
        'list_display': ('id', 'user_name', 'name', 'age', 'active', 'created_at', 'updated_at')
    }
    model_sharding.register_admin_opts(User._meta.label_lower, admin_opts)

    for sharding in User.get_sharding_list():
        model_sharding.create_model(User, sharding)


init_user_models()


class Log(models.Model, model_sharding.ShardingMixin):
    level = models.PositiveSmallIntegerField(default=0)
    content = models.TextField()
    time = models.DateTimeField(auto_now_add=True)

    # Date-based sharding
    SHARDING_TYPE = 'date'
    SHARDING_DATE_START = '2020-03-01'
    SHARDING_DATE_FORMAT = '%Y%m'

    def __str__(self):
        return "%s %s %s" % (self.time, self.level, self.content)

    class Meta:
        abstract = True
        db_table = "log_"


def init_log_models():
    admin_opts = {
        'list_display': ('id', 'time', 'level', 'content')
    }
    model_sharding.register_admin_opts(Log._meta.label_lower, admin_opts)

    for sharding in Log.get_sharding_list():
        model_sharding.create_model(Log, sharding)


init_log_models()
