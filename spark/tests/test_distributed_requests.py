import unittest
import json
import os
from pyspark.sql import SparkSession
from jobs.distributed_requests import get_asteroid_data, transform_data, generate_request_params
from datetime import datetime

class PySparkTest(unittest.TestCase):
        
    def setUp(self):
        """ Inicializa a sessao spark e os parametros que serao utilizados nos testes
        """
        self.spark = SparkSession.builder.appName("extract_asteroids_data").getOrCreate()
        self.api_key  = os.environ["API_KEY"]
        self.params   = {'start_date':datetime.strptime('2020-01-01', '%Y-%m-%d').date(),
                          'end_date':datetime.strptime('2020-01-01', '%Y-%m-%d').date(),
                          'api_key':self.api_key}

    def tearDown(self):
        """Finaliza a sessao spark
        """
        self.spark.stop()

    def test_generate_request_params(self):
      """ Testa o metodo generate_request_params
      """
      expected = [self.params]
      params =  generate_request_params(self.api_key
                                        ,datetime.strptime('2020-01-01', '%Y-%m-%d')
                                        ,datetime.strptime('2020-01-01', '%Y-%m-%d'))

      self.assertEqual(expected, params)

    def test_get_asteroid_data(self):
        """ Testa o metodo get_asteroid_data
        """
        data = json.loads(get_asteroid_data(self.params) )
        self.assertGreater(data['element_count'],0)

    def test_transform_data(self):
        """ Testa o metodo transform_data
        """
        expected = self.spark.createDataFrame(
                                [(11,())],
                                ['element_count', '2020-01-01'])
        rdd = self.spark.sparkContext.parallelize([self.params]) \
              .map(lambda param: get_asteroid_data(params=param))
        data = transform_data(self.spark, rdd) 

        self.assertEqual(data.filter("element_count=11").count(), expected.filter("element_count=11").count())
