import math
from hashlib import md5

from django.forms.models import model_to_dict
from django.http import JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.generic.base import View

from . import models


class JSONResponseMixin(object):
    response_class = JsonResponse
    params = {'ensure_ascii': False}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ret = {
            'message': '',
            'result': '',
            'status_code': 403
        }
        self.response_kwargs = {}

    def render_to_response(self, context):
        return self.response_class(context, json_dumps_params=self.params, **self.response_kwargs)


class UserView(JSONResponseMixin, View):
    @method_decorator(csrf_exempt)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)

    def get(self, request, *args, **kwargs):
        if request.GET.get('user_name', None):
            user_name = request.GET['user_name']
            digest = int(md5(user_name.encode()).hexdigest(), base=16)
            qs = models.User.shard(digest).objects.filter(user_name=user_name)
            if not qs.exists():
                self.ret['status_code'] = 404
                self.ret['message'] = '用户不存在'
            else:
                user = qs.get()
                self.ret['status_code'] = 200
                self.ret['result'] = model_to_dict(user)
        else:
            page_size = int(request.GET.get('page_size', 0)) or 10
            page = int(request.GET.get('page', 0)) or 1
            pagination_info = models.User.paginate_sharding(page, page_size)
            self.ret['status_code'] = 200
            self.ret.update(pagination_info)

        return self.render_to_response(self.ret)

    def post(self, request, *args, **kwargs):
        if 'user_name' in request.POST:
            user_name = request.POST['user_name']
            name = request.POST.get('name', user_name)
            digest = int(md5(user_name.encode()).hexdigest(), base=16)
            try:
                user = models.User.shard(digest).objects.create(user_name=user_name, name=name)
            except Exception as exc:
                self.ret['status_code'] = 500
                self.ret['message'] = str(exc)
            else:
                self.response_kwargs['status'] = self.ret['status_code'] = 201
                self.ret['result'] = model_to_dict(user)
        else:
            self.ret['message'] = '请求错误，缺少user_name参数'
            self.ret['status_code'] = 400

        return self.render_to_response(self.ret)

    def put(self, request, *args, **kwargs):
        if 'user_name' in request.GET:
            user_name = request.GET['user_name']
            update_map = {}
            if request.GET.get('name'):
                update_map['name'] = request.GET['name']
            if request.GET.get('age'):
                update_map['age'] = int(request.GET['age'])
            if request.GET.get('active'):
                update_map['active'] = request.GET['active']

            digest = int(md5(user_name.encode()).hexdigest(), base=16)
            user_model = models.User.shard(digest)
            try:
                user = user_model.objects.get(user_name=user_name)
                for k, v in update_map.items():
                    setattr(user, k, v)
                user.save(update_fields=update_map.keys())
            except user_model.DoesNotExist:
                self.ret['status_code'] = 404
                self.ret['message'] = '用户不存在'
            except Exception as exc:
                self.ret['status_code'] = 500
                self.ret['message'] = str(exc)
            else:
                self.ret['status_code'] = 200
                self.ret['result'] = model_to_dict(user)
        else:
            self.ret['message'] = '请求错误，缺少user_name参数'
            self.ret['status_code'] = 400

        return self.render_to_response(self.ret)

    def delete(self, request, *args, **kwargs):
        if 'user_name' in request.GET:
            user_name = request.GET['user_name']
            digest = int(md5(user_name.encode()).hexdigest(), base=16)
            user_model = models.User.shard(digest)
            try:
                user = user_model.objects.get(user_name=user_name)
                user.delete()
            except user_model.DoesNotExist:
                self.ret['status_code'] = 404
                self.ret['message'] = '用户不存在'
            except Exception as exc:
                self.ret['status_code'] = 500
                self.ret['message'] = str(exc)
            else:
                self.ret['status_code'] = 204
                self.ret['result'] = 'ok'
        else:
            self.ret['message'] = '请求错误，缺少user_name参数'
            self.ret['status_code'] = 400

        return self.render_to_response(self.ret)


class LogView(JSONResponseMixin, View):
    @method_decorator(csrf_exempt)
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)

    def get(self, request, *args, **kwargs):
        if request.GET.get('date', None):
            log_model = models.Log.shard(request.GET['date'])
        else:
            log_model = models.Log.shard()

        qs = log_model.objects.all()
        if request.GET.get('id', None):
            qs = qs.filter(id=request.GET['id'])
            if not qs.exists():
                self.ret['status_code'] = 404
                self.ret['message'] = '日志不存在'
            else:
                self.ret['status_code'] = 200
                self.ret['result'] = model_to_dict(qs.get())
        else:
            page_size = int(request.GET.get('page_size', 0)) or 10
            page = int(request.GET.get('page', 0)) or 1

            count = qs.count()
            max_page = math.ceil(count / page_size) or 1
            if page > max_page:
                page = max_page

            start = (page - 1) * page_size
            end = page * page_size
            qs = qs[start:end]

            result = []
            for log in qs:
                result.append(model_to_dict(log))

            self.ret['status_code'] = 200
            self.ret['result'] = result
            self.ret['count'] = count
            self.ret['next_page'] = page + 1 if page < max_page else -1

        return self.render_to_response(self.ret)

    def post(self, request, *args, **kwargs):
        if request.GET.get('date', None):
            log_model = models.Log.shard(request.GET['date'])
        else:
            log_model = models.Log.shard()

        if 'content' in request.POST:
            content = request.POST['content']
            level = request.POST.get('level', 0)
            try:
                log = log_model.objects.create(level=level, content=content)
            except Exception as exc:
                self.ret['status_code'] = 500
                self.ret['message'] = str(exc)
            else:
                self.response_kwargs['status'] = self.ret['status_code'] = 201
                self.ret['result'] = model_to_dict(log)
        else:
            self.ret['message'] = '请求错误，缺少content参数'
            self.ret['status_code'] = 400

        return self.render_to_response(self.ret)

    def delete(self, request, *args, **kwargs):
        if request.GET.get('date', None):
            log_model = models.Log.shard(request.GET['date'])
        else:
            log_model = models.Log.shard()

        if 'id' in request.GET:
            log_id = request.GET['id']
            try:
                log = log_model.objects.get(id=log_id)
                log.delete()
            except log_model.DoesNotExist:
                self.ret['status_code'] = 404
                self.ret['message'] = '日志不存在'
            except Exception as exc:
                self.ret['status_code'] = 500
                self.ret['message'] = str(exc)
            else:
                self.ret['status_code'] = 204
                self.ret['result'] = 'ok'
        else:
            self.ret['message'] = '请求错误，缺少content参数'
            self.ret['status_code'] = 400

        return self.render_to_response(self.ret)
