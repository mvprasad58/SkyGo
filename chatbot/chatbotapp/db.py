#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct  6 10:51:27 2017

@author: rahul
"""
import configparser
import logging
import os
import sys
import datetime
from pymongo import MongoClient
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, MetaData
from sqlalchemy import select
from sqlalchemy import update
import numpy as np

class skygodb:

    def __init__(self):
        config = configparser.ConfigParser()
        config.read('/home/mv/PycharmProjects/chat/chatbot/chatbotapp/settings.ini')
        self.logger = logging.getLogger(__name__)
        self.engine = create_engine('mysql+pymysql://mprasad:mprasad@123@172.16.1.225:3306/flights_info')
        self.connection = self.engine.connect()
        self.metadata = MetaData()
        self.flightsfinal = Table('flightsfinalnew', self.metadata, autoload=True, autoload_with=self.engine)
        self.zomato = Table('zomato', self.metadata, autoload=True, autoload_with=self.engine)



    def get_time(self,v):
        v = str(v)
        if len(v) == 4:
            first_part = v[:2]
            last_part = v[2:]
            time = first_part + ':' + last_part
            return time
        elif len(v) == 3:
            first_part = v[:1]
            last_part = v[1:]
            time = first_part + ':' + last_part
            return time
        else:
            return np.nan

    def departure(self,UniqueCarrier,FlightNum):
        try:
            stmt = select([self.flightsfinal.c.DepTime])
            stmt = stmt.where(self.flightsfinal.columns.UniqueCarrier == str(UniqueCarrier) and self.flightsfinal.columns.FlightNum == FlightNum)
            results = self.connection.execute(stmt).fetchone()[0]
            ti = self.get_time(results)

            return ti
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def FlightStatus(self,UniqueCarrier,FlightNum):
        try:
            stmt = select([self.flightsfinal.c.DepTime,self.flightsfinal.c.ArrTime])
            stmt = stmt.where(self.flightsfinal.columns.UniqueCarrier == str(UniqueCarrier) and self.flightsfinal.columns.FlightNum == FlightNum)
            results = self.connection.execute(stmt).fetchone()
            DepTime = self.get_time(results[0])
            ArrTime = self.get_time(results[1])
            s = datetime.datetime.strptime(str(DepTime), "%H:%M")
            t = datetime.datetime.strptime(str(ArrTime), "%H:%M")
            duration = t-s
            second = int(duration.seconds/3600)
            return second,DepTime,ArrTime
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def Food_Restaurant(self,cuisine):
        out = str(cuisine)+" not available"
        try:
            stmt = select([self.zomato.c.Restaurant_Name])
            stmt = stmt.where(self.zomato.columns.Cuisines.like('%'+str(cuisine)+'%'))
            results = self.connection.execute(stmt).fetchall()
            if len(results)>=3:
                one = results[0:3][0][0]
                two = results[0:3][1][0]
                three = results[0:3][2][0]
                # out = Yes indeed. There are two Starbucks one at Gate number A06 and the other at gate number B17.
                out = "There are three restaurants serving "+ str(cuisine)+" cuisines. "+ str(one)+" near Gate number 25, "+ str(two)+" beside Gate number 33 and "+str(three)+" opposite gate number 64."
            else:
                one = results[0:3][0][0]
                out = "There is only one restaurant serving " + str(cuisine) + " cuisines." + str(
                    one) + " near Gate number 25"
            return out
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def Find_Restaurant(self,restaurant):
        out = str(restaurant)+" not available"
        try:
            stmt = select([self.zomato.c.Restaurant_Name])
            stmt = stmt.where(self.zomato.columns.Restaurant_Name == str(restaurant))
            results = self.connection.execute(stmt).fetchall()
            if len(results)>=3:
                # one = results[0:3][0][0]
                # two = results[0:3][1][0]
                # three = results[0:3][2][0]
                out = "Yes indeed. There are two "+ str(restaurant)+" one at Gate number A06 and the other at gate number B17."
            else:
                # one = results[0:3][0][0]
                out = "Yes indeed. There is one "+ str(restaurant)+" one at Gate number A06."
            return out
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def Food_rating(self,restaurant):
        try:
            stmt = select([self.zomato.c.Aggregate_rating,self.zomato.c.Rating_text])
            stmt = stmt.where(self.zomato.columns.Restaurant_Name == str(restaurant))
            results = self.connection.execute(stmt).fetchone()
            Aggregate_rating = results[0]
            Rating_text = results[1]
            out = "The food at "+str(restaurant)+" seems "+ str(Rating_text)+". Most guests have rated it as "+ str(Rating_text)+" and it has an average rating of "+ str(Aggregate_rating) +" out of 5."
            return out
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def FlightCancellation(self,FlightNum,UniqueCarrier):
        try:
            stmt = select([self.flightsfinal.c.WeatherDelay])
            stmt = stmt.where(self.flightsfinal.columns.Cancelled == 1)
            results = self.connection.execute(stmt).fetchone()[0]
            out = "Your flight to Baltimore-Washington International airport, " +str(UniqueCarrier)+str(FlightNum)+", has been cancelled due to bad weather and reduced visibility. I can check the next possible flight to Baltimore for you?"
            return out
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def depart_gate_hindi(self,UniqueCarrier,FlightNum):
        try:
            stmt = select([self.flightsfinal.c.DepTime])
            stmt = stmt.where(self.flightsfinal.columns.UniqueCarrier == str(UniqueCarrier) and self.flightsfinal.columns.FlightNum == FlightNum)
            results = self.connection.execute(stmt).fetchone()[0]
            ti = self.get_time(results)
            out = str(UniqueCarrier)+str(FlightNum)+ " will depart from gate number 21 at "+ str(ti)+"."
            return out
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None






class databaseOperations:
    '''
    class databaseOperations
    '''

    def __init__(self):
        '''
        constructor
        :param self
        access all parameter to connect mongodb  from setting.ini file
        '''
        config = configparser.ConfigParser()
        config.read('/home/mv/PycharmProjects/chat/chatbot/chatbotapp/settings.ini')
        self.logger = logging.getLogger(__name__)
        self.user = config.get('mongo', 'username')
        self.port = int(config.get('mongo', 'port'))
        self.dbname = config.get('mongo', 'dbname')
        self.password = config.get('mongo', 'password')
        self.hostname = config.get('mongo', 'hostname')
        self.client = None
        self.connect_mongodb()

        # connect creation to mongodb

    def connect_mongodb(self):
        try:
            self.client = MongoClient(self.hostname, self.port)
            self.client.admin.authenticate(self.user, self.password, mechanism='SCRAM-SHA-1', source=self.dbname)
            self.logger.info("Connected to database server....")
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)


    def find_meter_date_active_consum(self, collection_name, meter_id,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"medidor_id": float(meter_id)},{'data' + '.' + str(year) + '.' + str(month)+'.'+'active_consum': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def find_meter_date_age_of_meter(self, collection_name, meter_id,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"medidor_id": float(meter_id)},{'data' + '.' + str(year) + '.' + str(month)+'.'+'age_of_meter': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None


    def find_meter_date_loss_likelihood(self, collection_name, meter_id,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"medidor_id": float(meter_id)},{'data' + '.' + str(year) + '.' + str(month)+'.'+'loss_likelihood': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def find_meter_date_reactve_consumption(self, collection_name, meter_id,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"medidor_id": float(meter_id)},{'data' + '.' + str(year) + '.' + str(month)+'.'+'reactive_consum': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def find_meter_date_stddeviation(self, collection_name, meter_id,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"medidor_id": float(meter_id)},{'data' + '.' + str(year) + '.' + str(month)+'.'+'std': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None


    def find_meter_date_totconsumption(self, collection_name, meter_id,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"medidor_id": float(meter_id)},{'data' + '.' + str(year) + '.' + str(month)+'.'+'total_consum': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None


    def find_substation_date_substation_active_consumption(self, collection_name, subcode,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"subestacion": str(subcode)},{'data' + '.' + str(year) + '.' + str(month)+'.'+'substation_active_consumption': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None


    def find_substation_date_substation_consumption(self, collection_name, subcode,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"subestacion": str(subcode)},{'data' + '.' + str(year) + '.' + str(month)+'.'+'substation_consumption': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def find_substation_date_substation_loss_likelihood(self, collection_name, subcode,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"subestacion": str(subcode)},{'data' + '.' + str(year) + '.' + str(month)+'.'+'loss_likelihood': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None


    def find_substation_date_substation_reactive_consumption(self, collection_name, subcode,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"subestacion": str(subcode)},{'data' + '.' + str(year) + '.' + str(month)+'.'+'substation_reactive_consumption': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None


    def find_substation_STransConnected(self, collection_name, subcode):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({'subestacion':subcode},{'subestacion':1,'transformador_id':1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def find_substation_date_STransConsumption(self, collection_name, subcode,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"subestacion": str(subcode)},{'data' + '.' + str(year) + '.' + str(month)+'.'+'trans_consumption': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def find_transformer_date_TActiveCons(self, collection_name, transcode,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"transformador_id": transcode},{'data' + '.' + str(year) + '.' + str(month)+'.'+'totalizer_active_consum': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def find_transformer_date_TLossLikelihood(self, collection_name, transcode,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"transformador_id": transcode},{'data' + '.' + str(year) + '.' + str(month)+'.'+'loss_likelihood': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None


    def find_transformer_date_TLossPercentage(self, collection_name, transcode,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"transformador_id": transcode},{'data' + '.' + str(year) + '.' + str(month)+'.'+'loss_percentage': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def find_transformer_date_TMeterCons(self, collection_name, transcode,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"transformador_id": transcode},{'data' + '.' + str(year) + '.' + str(month)+'.'+'meter_con_act': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def find_meter_TMetersConnected(self, collection_name, transcode):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({'transformador_id':transcode},{'medidor_id':1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def find_transformer_date_TReactiveCons(self, collection_name, transcode,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"transformador_id": transcode},{'data' + '.' + str(year) + '.' + str(month)+'.'+'totalizer_reactive_consum': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None


    def find_transformer_date_TStdDeviation(self, collection_name, transcode,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"transformador_id": transcode},{'data' + '.' + str(year) + '.' + str(month)+'.'+'standard_deviation': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def find_transformer_date_TTotConsumption(self, collection_name, transcode,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"transformador_id": transcode},{'data' + '.' + str(year) + '.' + str(month)+'.'+'totalizer_con_act': 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None











    def find_meters_all(self, collection_name):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({})
            #            for i in data:
            #
            #                print(i)
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None


    def find_meter(self, collection_name, meter_id):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"medidor_id": float(meter_id)})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def find_meter_date(self, collection_name, meter_id,year,month):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"medidor_id": float(meter_id)},{'data' + '.' + str(year) + '.' + str(month): 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def find_meter_zone(self, collection_name, meter_id):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"medidor_id": int(meter_id)})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None

    def find_meter_zoneContext(self, collection_name, meter_id,year):

        '''
        find document using a key from give collection
        :param collection_name:
        :param key:
        :return:none

        '''

        try:
            db = self.client[self.dbname]
            collection = db[str(collection_name)]
            data = collection.find({"medidor_id": int(meter_id)},{'data' + '.' + str(year): 1})
            # print(dumps(data))
            return data
        except Exception as e:
            self.logger.info("Could not find the document or record ")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return None




    # def find_pdf_record(self, collection_name, filename):
    #
    #     '''
    #     find document using a key from give collection
    #     :param collection_name:
    #     :param key:
    #     :return:none
    #
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         filename = str(filename)
    #         collection.find({"filename": filename})
    #         #            for i in data:
    #         #
    #         #                print(i)
    #         return True
    #
    #     except Exception as e:
    #         self.logger.info("Could not find the document or record ")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #         return False
    #
    # def find_record_in_doctable(self, collection_name, status):
    #
    #     '''
    #     find document using a key from give collection
    #     :param collection_name:
    #     :param key:
    #     :return:none
    #
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         data = collection.find({"status": status})
    #         #            for i in data:
    #         #
    #         #                print(i)
    #         return data
    #     except Exception as e:
    #         self.logger.info("Could not find the document or record ")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #         return None
    #
    # def find_record_in_doctable_or_case(self, collection_name, status1, status2):
    #
    #     '''
    #     find document using a key from give collection
    #     :param collection_name:
    #     :param key:
    #     :return:none
    #
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         data = collection.find({"status": {"$in": [status1, status2]}})
    #         #            for i in data:
    #         #
    #         #                print(i)
    #         return data
    #     except Exception as e:
    #         self.logger.info("Could not find the document or record ")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #         return None
    #
    # def find_application(self, collection_name, appno):
    #
    #     '''
    #     find document using a key from give collection
    #     :param collection_name:
    #     :param key:
    #     :return:none
    #
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         data = collection.find({"appno": appno})
    #         #            for i in data:
    #         #
    #         #                print(i)
    #         return data
    #     except Exception as e:
    #         self.logger.info("Could not find the document or record ")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #         return None
    #
    # def find_application_uid(self, collection_name, uid):
    #
    #     '''
    #     find document using a key from give collection
    #     :param collection_name:
    #     :param key:
    #     :return:none
    #
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         data = collection.find({"uid": uid})
    #         #            for i in data:
    #         #
    #         #                print(i)
    #         return data
    #     except Exception as e:
    #         self.logger.info("Could not find the document or record ")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #         return None
    #
    # def insert_into_collection(self, collection_name, AppNo, appdata, intime, acc_status_code, gen_excel_status,
    #                            no_of_docs, authkey):
    #     '''
    #     insertion of document
    #     :param collection_name:
    #     :param key:
    #     :param data:
    #     :return: none
    #     '''
    #     #
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #
    #         collection.insert_one(
    #             {"AppNo": AppNo, "appdata": appdata, "intime": intime, "acc_status_code": acc_status_code,
    #              "gen_excel_status": gen_excel_status, "no_of_docs": no_of_docs, "authkey": authkey})
    #         self.logger.info("Record inserted")
    #     except Exception as e:
    #         self.logger.info("Record insertion failed")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #
    # def insert_into_collection_tab2(self, collection_name, uid, pid, wid, Appno, Sname, intime, stime, dur, status,
    #                                 bankname, language, authkey, noofpages, message, drymode, accuracy,
    #                                 no_of_corrections, editaccstatus, editurl, luigi_status_code, doc_pull_intime,
    #                                 doc_pull_stime):
    #     '''
    #     insertion of document
    #     :param collection_name:
    #     :param key:
    #     :param data:
    #     :return: none
    #     '''
    #     #
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #
    #         collection.insert_one(
    #             {"uid": uid, "pid": pid, "wid": wid, "appno": Appno, "sname": Sname, "intime": intime, "stime": stime,
    #              "dur": dur, "status": status, "bankname": bankname, "language": language, "authkey": authkey,
    #              "no_of_pages": noofpages, "message": message, "drymode": drymode, "accuracy": accuracy,
    #              "no_of_corrections": no_of_corrections, "edit_acc_status": editaccstatus, "edit_url": editurl,
    #              "luigi_status_code": luigi_status_code, "doc_pull_intime": doc_pull_intime,
    #              "doc_pull_stime": doc_pull_stime})
    #         self.logger.info("Record inserted")
    #     except Exception as e:
    #         self.logger.info("Record insertion failed")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #
    # def update_collection(self, collection_name, key, value, job_pull_intime, job_pull_stime):
    #     '''
    #     update collections's document using key but key
    #     :param collection_name:
    #     :param key:
    #     :param value:
    #     :return: none
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         collection.update_one({"appno": key}, {
    #             "$set": {"acc_status_code": value, "job_pull_intime": job_pull_intime,
    #                      "job_pull_stime": job_pull_stime}})
    #         self.logger.info("records are updated")
    #     except Exception as e:
    #         self.logger.info("Records are not updated")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #
    # def update_collection_doctable_status_sname(self, collection_name, uid_key, status, sname):
    #     '''
    #     update collections's document using key but key
    #     :param collection_name:
    #     :param key:
    #     :param value:
    #     :return: none
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         collection.update_one({"uid": uid_key}, {"$set": {"status": status, "sname": sname}})
    #         self.logger.info("records are updated")
    #     except Exception as e:
    #         self.logger.info("Records are not updated")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #
    # def update_collection_doctable_pid_status_acc_noofcorr_msg_editacc_editurl_sname(self, collection_name, uid_key,
    #                                                                                  pid, status, accuracy,
    #                                                                                  no_of_corrections, message,
    #                                                                                  editaccstatus, editurl, sname):
    #     '''
    #     update collections's document using key but key
    #     :param collection_name:
    #     :param key:
    #     :param value:
    #     :return: none
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         collection.update_one({"uid": uid_key}, {
    #             "$set": {"pid": pid, "status": status, "accuracy": accuracy, "no_of_corrections": no_of_corrections,
    #                      "message": message, "edit_acc_status": editaccstatus, "edit_url": editurl, "sname": sname}})
    #         self.logger.info("records are updated")
    #     except Exception as e:
    #         self.logger.info("Records are not updated")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #
    # def update_collection_doctable_based_on_ocr_response(self, collection_name, uid_key, message, statusCode,
    #                                                      no_of_pages, sname):
    #     '''
    #     update collections's document using key but key
    #     :param collection_name:
    #     :param key:
    #     :param value:
    #     :return: none
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         collection.update_one({"uid": uid_key}, {
    #             "$set": {"message": message, "status": statusCode, "no_of_pages": no_of_pages, "sname": sname}})
    #         self.logger.info("records are updated")
    #     except Exception as e:
    #         self.logger.info("Records are not updated")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #
    # def update_collection_doctables_doc_pull_in_and_stime(self, collection_name, uid_key, doc_pull_intime,
    #                                                       doc_pull_stime):
    #     '''
    #     update collections's document using key but key
    #     :param collection_name:
    #     :param key:
    #     :param value:
    #     :return: none
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         collection.update_one({"uid": uid_key},
    #                               {"$set": {"doc_pull_intime": doc_pull_intime, "doc_pull_stime": doc_pull_stime}})
    #         self.logger.info("records are updated")
    #     except Exception as e:
    #         self.logger.info("Records are not updated")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #
    # def sort_collection(self, collection_name, acc_status_code):
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         data = collection.find({"acc_status_code": acc_status_code}).sort(
    #             [("intime", 1)])  # intime ascending and status decending
    #         return data
    #     except Exception as e:
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #         return None
    #
    # def sort_collection_doc_table(self, collection_name, status):
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         data = collection.find({"status": status}).sort([("intime", 1)])  # intime ascending and status decending
    #         return data
    #     except Exception as e:
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #         return None
    #
    # def remove_document(self, collection_name, key):
    #     '''
    #     remove document from collection given a key
    #     :param collection_name:
    #     :param key:
    #     :return:
    #     '''
    #
    #     try:
    #         db = self.client[self.dbname]
    #         collection = db[str(collection_name)]
    #         collection.remove_one({"uid": key})
    #         self.logger.info("selected document are removed")
    #     except Exception as e:
    #         self.logger.info("Document could not be deleted")
    #         self.logger.error(str(e))
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         self.logger.error(exc_type, fname1, exc_tb.tb_lineno)


# db_obj = databaseOperations()
# from bson.json_util import dumps
# import ast
# x = db_obj.find_meter_date('meters',2077688,2011,12)
# appdata = dumps(x)
# appdata = ast.literal_eval(appdata)