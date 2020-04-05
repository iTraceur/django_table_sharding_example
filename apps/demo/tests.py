from hashlib import md5

from django.conf import settings
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from django.utils.http import urlencode

from apps.demo import models


class TestUnit(TestCase):
    def test_constant_based_sharding(self):
        user_name = 'iTraceur'
        digest = int(md5(user_name.encode()).hexdigest(), base=16)
        models.User.shard(digest).objects.create(user_name=user_name, name='iTraceur', age=18)
        self.assertTrue(models.User.shard(digest).objects.filter(user_name=user_name).exists())

        for i in range(models.User.SHARDING_COUNT):
            user_name = 'iTraceur-' + str(i)
            name = 'iTraceur-%d Zhao' % i
            age = 18 + i
            digest = int(md5(user_name.encode()).hexdigest(), base=16)
            models.User.shard(digest).objects.create(user_name=user_name, name=name, age=age)
            self.assertTrue(models.User.shard(digest).objects.filter(user_name=user_name).exists())

        user_name = 'iTraceur-9'
        digest = int(md5(user_name.encode()).hexdigest(), base=16)
        self.assertTrue(models.User.shard(digest).objects.filter(user_name=user_name).exists())
        user_name = 'iTraceur-1'
        digest = int(md5(user_name.encode()).hexdigest(), base=16)
        self.assertTrue(models.User.shard(digest).objects.filter(user_name=user_name).exists())

        models.User.shard().objects.create(user_name='iTraceurZhao', name='iTraceur Zhao')
        self.assertTrue(models.User.shard().objects.filter(user_name='iTraceurZhao').exists())
        models.User.shard().objects.filter(user_name='iTraceurZhao').delete()
        self.assertFalse(models.User.shard().objects.filter(user_name='iTraceurZhao').exists())

        url = reverse('demo:user')
        user_name = 'iTraceur-test'
        digest = int(md5(user_name.encode()).hexdigest(), base=16)
        data = {
            'user_name': user_name,
            'name': 'iTraceur from post',
            'age': 18
        }
        response = self.client.post(url, data)
        self.assertEqual(response.status_code, 201)
        self.assertEqual(models.User.shard(digest).objects.filter(user_name=user_name).count(), 1)

        data = {
            'user_name': user_name,
            'name': 'iTraceur updated',
            'age': 20
        }
        params = {
            'QUERY_STRING': urlencode(data, doseq=True),
        }
        response = self.client.put(url, **params)
        self.assertEqual(response.json()['result']['user_name'], user_name)
        self.assertEqual(response.json()['result']['name'], 'iTraceur updated')
        self.assertEqual(response.json()['result']['age'], 20)

        response = self.client.get(url, data={'user_name': user_name})
        self.assertEqual(response.json()['result']['user_name'], user_name)
        self.assertEqual(models.User.shard(digest).objects.get(user_name=user_name).name, 'iTraceur updated')
        self.assertEqual(models.User.shard(digest).objects.get(user_name=user_name).age, 20)

        params = {
            'QUERY_STRING': urlencode({'user_name': user_name}, doseq=True),
        }
        response = self.client.delete(url, **params)
        self.assertEqual(response.json()['status_code'], 204)
        self.assertEqual(models.User.shard(digest).objects.filter(user_name=user_name).count(), 0)

    def test_date_based_sharding(self):
        models.Log.shard().objects.create(content='test_date_based_sharding')
        self.assertTrue(models.Log.shard().objects.filter(content='test_date_based_sharding').exists())

        for sharding in models.Log.get_sharding_list():
            content = 'test_date_based_sharding' + sharding
            models.Log.shard(sharding).objects.create(content=content)
            self.assertTrue(models.Log.shard(sharding).objects.filter(content=content).exists())

        date_start_str = getattr(settings, 'SHARDING_DATE_START', '2020-01-01')
        default_date = timezone.datetime.strptime('2020-01-01', '%Y-%m-%d')
        date_start = timezone.datetime.strptime(date_start_str, '%Y-%m-%d')
        self.assertLessEqual(date_start, default_date)

        models.Log.shard(202001).objects.create(content='test_date_based_sharding single 202001')
        self.assertTrue(models.Log.shard(202001).objects.filter(content='test_date_based_sharding single 202001').exists())

        models.Log.shard(202002).objects.create(content='test_date_based_sharding single 202002')
        self.assertTrue(models.Log.shard(202002).objects.filter(content='test_date_based_sharding single 202002').exists())

        models.Log.shard(202003).objects.create(content='test_date_based_sharding single 202003')
        self.assertTrue(models.Log.shard('202003').objects.filter(content='test_date_based_sharding single 202003').exists())

        models.Log.shard('202004').objects.create(content='test_date_based_sharding single 202004')
        self.assertTrue(models.Log.shard('202004').objects.filter(content='test_date_based_sharding single 202004').exists())

        models.Log.shard('190001').objects.create(content='test_date_based_sharding single 190001')
        self.assertTrue(models.Log.shard().objects.filter(content='test_date_based_sharding single 190001').exists())
        self.assertTrue(models.Log.shard(190001).objects.filter(content='test_date_based_sharding single 190001').exists())
        models.Log.shard(190001).objects.filter(content='test_date_based_sharding single 190001').delete()
        self.assertFalse(models.Log.shard().objects.filter(content='test_date_based_sharding single 190001').exists())
        self.assertFalse(models.Log.shard('190001').objects.filter(content='test_date_based_sharding single 190001').exists())

        url = reverse('demo:log')
        content = 'test log'
        response = self.client.post(url, {'content': 'test log'})
        self.assertEqual(response.status_code, 201)
        self.assertEqual(models.Log.shard().objects.filter(content=content).count(), 1)
        log_id = response.json()['result']['id']

        response = self.client.get(url, {'id': log_id})
        self.assertEqual(response.json()['result']['content'], content)

        params = {
            'QUERY_STRING': urlencode({'id': log_id}, doseq=True),
        }
        response = self.client.delete(url, **params)
        self.assertEqual(response.json()['status_code'], 204)
