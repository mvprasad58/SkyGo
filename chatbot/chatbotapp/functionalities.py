

from bson.json_util import dumps
import ast
from chatbotapp.db import databaseOperations
import numpy as np
import os,sys
import logging
from json import loads,dumps
import json
from bson import json_util

import pandas as pd


class chat_functionalities():


    def __init__(self):

        self.db_obj = databaseOperations()
        self.logger = logging.getLogger(__name__)



    def MactiveConsumption(self,meter_id,year,month):
        out = "Something went wrong!"
        try:
            month = str(month)
            meter_id = int(meter_id)
            active_consumption = self.db_obj.find_meter_date_active_consum('meters', meter_id,year,month)
            print(year, month, meter_id, type(year), type(month), type(meter_id))
            if not active_consumption:
                return out
            else:

                appdata = None
                for data in active_consumption:
                    appdata = loads(dumps(data))
                actve_cons = appdata['data'][str(year)][str(month)]['active_consum']
                # out = "The meter's active consumption is " + str(actve_cons) + " units."
                out = "El consumo activo del medidor es de "+str(actve_cons)+" unidades."
                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out


    def MAgeofMeter(self,meter_id,year,month):
        out = "Something went wrong!"
        try:
            month = str(month)
            meter_id = int(meter_id)
            meter_age = self.db_obj.find_meter_date_age_of_meter('meters', meter_id,year,month)
            if not meter_age:
                return out
            else:
                appdata = None
                for data in meter_age:
                    appdata = loads(dumps(data))
                out = appdata['data'][str(year)][str(month)]['age_of_meter']
                # out = "The meter's age is " + str(out) + " days."
                out = "La edad del medidor es "+ str(out)+" días."
                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out


    def MCalcLossLikelihood(self,meter_id,year,month):

        out = "Something went wrong!"
        try:
            month = str(month)
            meter_id = int(meter_id)
            meter_loss_likelihood = self.db_obj.find_meter_date_loss_likelihood('meters', meter_id, year, month)
            if not meter_loss_likelihood:
                return out
            else:

                appdata = None
                for data in meter_loss_likelihood:
                    appdata = loads(dumps(data))
                out = appdata['data'][str(year)][str(month)]['loss_likelihood']
                out = "The loss likelihood is " + str(out) + ".\nTo find information about another meter/transformer/substation or a different month-year,please say OK."
                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out


    def MReactiveConsumption(self,meter_id,year,month):

        out = "Something went wrong!"
        try:
            month = str(month)
            meter_id = int(meter_id)
            meter_reactive_consumption = self.db_obj.find_meter_date_reactve_consumption('meters', meter_id, year, month)
            if not meter_reactive_consumption:
                return out
            else:

                appdata = None
                for data in meter_reactive_consumption:
                    appdata = loads(dumps(data))
                out = appdata['data'][str(year)][str(month)]['reactive_consum']
                # out = "The meter's reactive consumption is " + str(out) + " units."
                out = "El consumo reactivo del medidor es "+str(out)+" unidades."
                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out

    def MStdDeviation(self,meter_id,year,month):

        out = "Something went wrong!"
        try:
            month = str(month)
            meter_id = int(meter_id)
            meter_stddeviation = self.db_obj.find_meter_date_stddeviation('meters', meter_id, year, month)
            if not meter_stddeviation:
                return out
            else:

                appdata = None
                for data in meter_stddeviation:
                    appdata = loads(dumps(data))
                out = appdata['data'][str(year)][str(month)]['std']
                out = "The meter's standard deviation is " + str(out) + " units.\nTo find information about another meter/transformer/substation or a different month-year,please say OK"
                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out

    def MTotConsumption(self,meter_id,year,month):

        out = "Something went wrong!"
        try:
            month = str(month)
            meter_id = int(meter_id)
            meter_totconsumption = self.db_obj.find_meter_date_totconsumption('meters', meter_id, year, month)
            if not meter_totconsumption:
                return out
            else:

                appdata = None
                for data in meter_totconsumption:
                    appdata = loads(dumps(data))
                out = appdata['data'][str(year)][str(month)]['total_consum']
                out = "The meter's consumption for the given month is " + str(out) + " units."
                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out

    def SActCons(self,substation,year,month):

        out = "Something went wrong!"
        appdata = None
        try:
            month = str(month)
            year = str(year)
            substation_code = str(substation)
            substation_active_consumption = self.db_obj.find_substation_date_substation_active_consumption('substations', substation_code, year, month)

            if not substation_active_consumption:
                return out
            else:

                appdata = json.loads(json_util.dumps(substation_active_consumption))
                #
                # appdata = None
                # for data in substation_active_consumption:
                #     appdata = loads(dumps(data))
                #     print(appdata)
                out = appdata[0]['data'][str(year)][str(month)]['substation_active_consumption']
                out = "The substation's active consumption for the given month-year is " + str(out)+ " units.\nTo find information about another meter/transformer/substation or a different month-year,please say OK"
                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out


    def SConsumption(self,substation,year,month):

        out = "Something went wrong!"
        appdata = None
        try:
            month = str(month)
            year = str(year)
            substation_code = str(substation)
            substation_consumption = self.db_obj.find_substation_date_substation_consumption('substations', substation_code, year, month)

            if not substation_consumption:
                return out
            else:

                appdata = json.loads(json_util.dumps(substation_consumption))

                # appdata = None
                # for data in substation_consumption:
                #     appdata = loads(dumps(data))
                out = appdata[0]['data'][str(year)][str(month)]['substation_consumption']
                out = "The substation's consumption for the given month-year is " + str(out) + " units.\nTo find information about another meter/transformer/substation or a different month-year,please say OK"
                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out



    def SLossLikelihood(self,substation,year,month):

        out = "Something went wrong!"
        appdata = None
        try:
            month = str(month)
            year = str(year)
            substation_code = str(substation)
            substation_loss_likelihood = self.db_obj.find_substation_date_substation_loss_likelihood('substations', substation_code, year, month)

            if not substation_loss_likelihood:
                return out
            else:

                appdata = json.loads(json_util.dumps(substation_loss_likelihood))

                # appdata = None
                # for data in substation_consumption:
                #     appdata = loads(dumps(data))
                out = appdata[0]['data'][str(year)][str(month)]['loss_likelihood']
                out = "The loss likelihood of the substation for the given month-year is " + str(out) + ".\nTo find information about another meter/transformer/substation or a different month-year,please say OK"
                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out


    def SReactCons(self,substation,year,month):

        out = "Something went wrong!"
        appdata = None
        try:
            month = str(month)
            year = str(year)
            substation_code = str(substation)
            substation_reactive_consumption = self.db_obj.find_substation_date_substation_reactive_consumption('substations', substation_code, year, month)

            if not substation_reactive_consumption:
                return out
            else:

                appdata = json.loads(json_util.dumps(substation_reactive_consumption))

                # appdata = None
                # for data in substation_consumption:
                #     appdata = loads(dumps(data))
                out = appdata[0]['data'][str(year)][str(month)]['substation_reactive_consumption']
                out = "The substation's reactive consumption for the given month-year is " + str(out) + " units.\nTo find information about another meter/transformer/substation or a different month-year,please say OK",
                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out

    def STransConnected(self,substation):

        out = "Something went wrong!"
        appdata = None
        try:
            substation_code = str(substation)
            substation_STransConnected = self.db_obj.find_substation_STransConnected('transformers', "PAVAS")

            if not substation_STransConnected:
                return out
            else:
                appdata = None
                d = []
                trans_list = []
                for data in substation_STransConnected:
                    appdata = loads(dumps(data))
                    d.append(appdata)
                    trans_list.append(appdata['transformador_id'])
                out = len(d)
                # x = ','.join(trans_list[:2])
                # print(x)
                # print(trans_list)
                x = [str(int(i)) for i in trans_list[:2]]
                x1 = ','.join(x)
                # out = "The " + str(len(x)) + " transformers connected to the given substation are " + str(x1) + ".", ##code need to change
                out = "Los "+str(len(x))+" transformadores conectados a la subestación dada son " +str(x1)+"."
                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out


    def STransConsumption(self,substation,year,month):

        out = "Something went wrong!"
        appdata = None
        try:
            month = str(month)
            year = str(year)
            substation_code = str(substation)
            substation_TransConsumption = self.db_obj.find_substation_date_STransConsumption('substations', substation_code, year, month)

            if not substation_TransConsumption:
                return out
            else:

                appdata = json.loads(json_util.dumps(substation_TransConsumption))

                # appdata = None
                # for data in substation_TransConsumption:
                #     appdata = loads(dumps(data))
                out = appdata[0]['data'][str(year)][str(month)]['trans_consumption']
                out = "The total consumption of all the transformers connected to the substation is " + str(out) + " units.\nIf you want to find information about another substation or during another month-year,please say OK"
                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out


    def TActiveCons(self,transformer,year,month):

        out = "Something went wrong!"
        appdata = None
        try:
            month = str(month)
            year = str(year)
            transformer_code = float(transformer)
            transformer_TActiveCons = self.db_obj.find_transformer_date_TActiveCons('transformers', transformer_code, year, month)

            if not transformer_TActiveCons:
                return out
            else:

                appdata = json.loads(json_util.dumps(transformer_TActiveCons))

                # appdata = None
                # for data in substation_TransConsumption:
                #     appdata = loads(dumps(data))
                out = appdata[0]['data'][str(year)][str(month)]['totalizer_active_consum']
                out = "The transformer's totalizer's active consumption is " + str(out) + " units.\nTo find information about another meter/transformer/substation or a different month-year,please say OK"
                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out

    def TLossLikelihood(self,transformer,year,month):

        out = "Something went wrong!"
        appdata = None
        try:
            month = str(month)
            year = str(year)
            transformer_code = float(transformer)
            transformer_TLossLikelihood = self.db_obj.find_transformer_date_TLossLikelihood('transformers', transformer_code, year, month)

            if not transformer_TLossLikelihood:
                return out
            else:

                appdata = json.loads(json_util.dumps(transformer_TLossLikelihood))

                # appdata = None
                # for data in substation_TransConsumption:
                #     appdata = loads(dumps(data))
                out = appdata[0]['data'][str(year)][str(month)]['loss_likelihood']
                # out = "The loss likelihood value is " + str(out) + " units."
                out = "El valor de probabilidad de pérdida es "+str(out)+" unidades."
                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out


    def TLossPercentage(self,transformer,year,month):

        out = "Something went wrong!"
        appdata = None
        try:
            month = str(month)
            year = str(year)
            transformer_code = float(transformer)
            transformer_TLossPercentage = self.db_obj.find_transformer_date_TLossPercentage('transformers', transformer_code, year, month)

            if not transformer_TLossPercentage:
                return out
            else:

                appdata = json.loads(json_util.dumps(transformer_TLossPercentage))

                # appdata = None
                # for data in substation_TransConsumption:
                #     appdata = loads(dumps(data))
                out = appdata[0]['data'][str(year)][str(month)]['loss_percentage']
                # out = "The percentage loss is " + str(out) + "."
                out = "El porcentaje de pérdida es "+str(out)+"."

                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out

    def TMeterCons(self,transformer,year,month):

        out = "Something went wrong!"
        appdata = None
        try:
            month = str(month)
            year = str(year)
            transformer_code = float(transformer)
            transformer_TMeterCons = self.db_obj.find_transformer_date_TMeterCons('transformers', transformer_code, year, month)

            if not transformer_TMeterCons:
                return out
            else:

                appdata = json.loads(json_util.dumps(transformer_TMeterCons))

                # appdata = None
                # for data in substation_TransConsumption:
                #     appdata = loads(dumps(data))
                out = appdata[0]['data'][str(year)][str(month)]['meter_con_act']
                out = "The combined consumption of all the meters of the transformer is " + str(out) + " units.\nTo find information about another meter/transformer/substation or a different month-year,please say OK"

                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out


    def TMetersConnected(self,transformer):

        out = "Something went wrong!"
        appdata = None
        try:
            transformer_code = int(transformer)
            meters_TMetersConnected = self.db_obj.find_meter_TMetersConnected('meters', transformer_code)



            if not meters_TMetersConnected:
                return out
            else:
                appdata = None
                d = []
                trans_list = []
                for data in meters_TMetersConnected:
                    appdata = loads(dumps(data))
                    d.append(appdata)
                    trans_list.append(appdata['medidor_id'])
                out = len(d)
                x = ','.join(trans_list[:5])
                # out = "The " + str(out) + " meters connected to the given transformer are" + str(x)+ "."
                out = "Los "+str(out)+" metros conectados al transformador dado son "+str(x)+"."
                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out

    def TReactiveCons(self,transformer,year,month):

        out = "Something went wrong!"
        appdata = None
        try:
            month = str(month)
            year = str(year)
            transformer_code = float(transformer)
            transformer_TReactiveCons = self.db_obj.find_transformer_date_TReactiveCons('transformers', transformer_code, year, month)

            if not transformer_TReactiveCons:
                return out
            else:

                appdata = json.loads(json_util.dumps(transformer_TReactiveCons))

                # appdata = None
                # for data in substation_TransConsumption:
                #     appdata = loads(dumps(data))
                out = appdata[0]['data'][str(year)][str(month)]['totalizer_reactive_consum']
                out = "The transformer's totalizer's reactive consumption is " + str(out) + " units.\nTo find information about another meter/transformer/substation or a different month-year,please say OK"

                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out


    def TStdDeviation(self,transformer,year,month):

        out = "Something went wrong!"
        appdata = None
        try:
            month = str(month)
            year = str(year)
            transformer_code = float(transformer)
            transformer_TStdDeviation = self.db_obj.find_transformer_date_TStdDeviation('transformers', transformer_code, year, month)

            if not transformer_TStdDeviation:
                return out
            else:

                appdata = json.loads(json_util.dumps(transformer_TStdDeviation))

                # appdata = None
                # for data in substation_TransConsumption:
                #     appdata = loads(dumps(data))
                out = appdata[0]['data'][str(year)][str(month)]['standard_deviation']
                out = "The standard deviation of the transformer is " + str(out) + " units.\nTo find information about another meter/transformer/substation or a different month-year,please say OK"

                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out

    def TTotConsumption(self,transformer,year,month):

        out = "Something went wrong!"
        appdata = None
        try:
            month = str(month)
            year = str(year)
            transformer_code = float(transformer)
            transformer_TTotConsumption = self.db_obj.find_transformer_date_TTotConsumption('transformers', transformer_code, year, month)

            if not transformer_TTotConsumption:
                return out
            else:

                appdata = json.loads(json_util.dumps(transformer_TTotConsumption))

                # appdata = None
                # for data in substation_TransConsumption:
                #     appdata = loads(dumps(data))
                out = appdata[0]['data'][str(year)][str(month)]['totalizer_con_act']
                out = "The totalizer consumption of the transformer is " + str(out) + " units.\nTo find information about another meter/transformer/substation or a different month-year,please say OK",

                return out
        except Exception as e:
            self.logger.info("Could not establish connection to database server....")
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return out


    def average_loss_likeli_per_zone_all_years(self,meter_id,meter_parameter):
        status = 'Something went wrong!'
        meter_id = int(meter_id)

        try:
            average_loss_likeli_per_zone_all_years_values = self.db_obj.find_meter_zone('meters', meter_id)
            appdata = json.loads(json_util.dumps(average_loss_likeli_per_zone_all_years_values))
            data = appdata[0]['data']
            years = list(data.keys())
            year_list = [int(i) for i in years]
            max_year = str(max(year_list))
            months1 = list(data[str(max_year)].keys())
            active_consumption_for_a_meter = [data[str(max_year)][str(months)][str(meter_parameter)] for months in list(data[str(max_year)].keys())]
            # active_consumption_for_a_meter = [data[str(i)][str(months)][str(meter_parameter)] for i in max_year for months in
            #                                   list(data[str(i)].keys())]
            npa = np.asarray(active_consumption_for_a_meter, dtype=np.double)
            out = np.mean(npa)
            status = "For the latest year that is"+" "+str(max_year)+", the average loss is" + str(out)+". Please let me know if you would like to know more about the previous years too?"
            return status

        except Exception as e:
            status = "Meter Id does not exist. please enter with proper Meter Id and zone number. Thank You"
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return status

    def average_loss_all_yearsC(self,meter_id,meter_parameter,year):
        status = 'Something went wrong!'
        meter_id = int(meter_id)

        try:
            average_loss_likeli_per_zone_all_years_values = self.db_obj.find_meter_zoneContext('meters', meter_id,year)
            appdata = json.loads(json_util.dumps(average_loss_likeli_per_zone_all_years_values))
            data = appdata[0]['data']
            # print(data)
            # years = list(data.keys())
            # print(years)
            # year_list = [int(i) for i in years]
            # max_year = str(sorted(year_list)[-2])
            active_consumption_for_a_meter = [data[str(year)][str(months)][str(meter_parameter)] for months in list(data[str(year)].keys())]
            # active_consumption_for_a_meter = [data[str(i)][str(months)][str(meter_parameter)] for i in max_year for months in
            #                                   list(data[str(i)].keys())]
            npa = np.asarray(active_consumption_for_a_meter, dtype=np.double)
            out = np.mean(npa)
            status = "For the previous year that is"+" "+str(year)+", the average "+str(meter_parameter)+" loss is " + str(out)
            return status

        except Exception as e:
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return status

    def more_losses_in_a_substation(self,substation): ###########hardcodded
        status = 'Something went wrong!'
        substation_id = str("PAVAS").upper()
        class_com = {'1': 'Residential',
                     '2': 'Commercial',
                     '3': 'Industrial',
                     '4': 'Official(Public Government Institutions)',
                     '5': 'Public Street lighting',
                     '6': 'Electricity brokers / companies and Charges SDL',
                     '7': 'Provisional',
                     '8': 'Water suppliers',
                     '9': 'Totalizadores',
                     '10': 'Common Areas',
                     '15': 'Energy in bulk',
                     '16': 'Start and Stop',
                     '17': 'Use of Networks',
                     '18': 'Charges STR'}

        try:
            df = pd.read_excel("/home/mv/dialogflow/chatbot/chatbotapp/Substationwise_loss.xlsx")

            df1 = df[df['subestacion'] == str(substation_id)]
            df1.sort_values(by = 'Loss_contribution',ascending=False,inplace=True)
            df1.reset_index(inplace=True,drop=True)
            df1 = df1[['clase_servicio','subestacion','Loss_contribution']]

            # z = ""
            c = []
            for index,column in df1.iterrows():

                re = column['clase_servicio']
                y = class_com[str(re)]
                print(re,y)
                # z = str(y) +" consumers contributed approximately " + str(
                #     int(column['Loss_contribution']))+"%" + " of the total losses" + ", "


                z = str(y) +" los consumidores contribuyeron aproximadamente" + str(
                    int(column['Loss_contribution']))+"%" + " of the total losses" + ", "
                c.append(z)


            # text_f = str(c[0])+" "+str(c[1])+" while other consumers such as industrial, street lighting made up the balance."
            text_f = str(c[0]) + " " + str(
                c[1]) + " mientras que otros consumidores como el alumbrado público e industrial componen el resto."

            # out = "In " + str(substation) +", " +text_f
            out = "En " + str(substation) + ", " + text_f
            return out
            # out = 'In Cuba, residential consumers contributed approximately 90% of the total losses, Commercial contributed 9% of the total losses while other consumers such as industrial, street lighting made up the balance.

        except Exception as e:
            self.logger.error(str(e))
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
            return status







    def active_consumption_all_years(self,meter_id):
        out = "Meter id doesn't exist"
        meter_id = int(meter_id)

        ########need to change
        if meter_id == 2216:
            out = "For the latest year that is 2017, the average consumption is 198 units. Please let me know if you would like to know more about the previous years?"
            return out
        else:
            try:
                average_active_consumption = self.db_obj.find_meter_zone('meters', meter_id)
                appdata = json.loads(json_util.dumps(average_active_consumption))
                data = appdata[0]['data']

                years = list(data.keys())
                year_list = [int(i) for i in years]
                max_year = str(max(year_list))
                months_list = list(data[str(max_year)].keys())
                # print(data[max_year])
                active_consumption_for_a_meter = [data[max_year][months]['active_consum'] for months in months_list]
                npa = np.asarray(active_consumption_for_a_meter, dtype=np.double)
                avg_value = str(int(np.mean(npa)))
                # out = "For the current year that is " + str(max_year) + "," + " the average consumption is" + str(
                #     avg_value) + " units. Please let me know if you would like to know more about the previous years?"

                out = "Para el año actual que es "+str(max_year)+", el consumo promedio es de "+str(avg_value)+" unidades. Por favor, avíseme si le gustaría saber más sobre los años anteriores."
                return  out
            except Exception as e:
                self.logger.error(str(e))
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
                return out

    # def active_cons_per_meter_per_Y_M(self,meter_id,year,month):
    #     status = 'Not Availble'
    #     print(meter_id,year,month)
    #     if str(meter_id).isdigit() and len(str(year))==4 and month in range(1,13):
    #         meter_id  = int(meter_id)
    #         appdata = self.db_obj.find_meter_date('meters', meter_id,year,month)
    #         appdata = dumps(appdata)
    #         appdata = ast.literal_eval(appdata)
    #
    #         try:
    #             out = appdata[0]['data'][str(year)][str(month)]['active_consum']
    #             status = "active consumption of"+" "+str(meter_id)+" "+"for a year"+ " "+str(year)+"for a month"+ " "+str(month)+"is"+" "+str(out)
    #         except Exception as e:
    #             status = "Meter Id or year or month does not exist. please enter with proper Meter Id or year or month. Thank You"
    #             self.logger.error(str(e))
    #             exc_type, exc_obj, exc_tb = sys.exc_info()
    #             fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #             self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #             return status
    #
    #     else:
    #         status = "Meter Id or year or month does not exist. please enter with proper Meter Id or year or month. Thank You"
    #
    #     return status
    #
    #
    # def loss_likelihood_all_years(self,meter_id):
    #     status = 'Not Availble'
    #     if str(meter_id).isdigit():
    #         meter_id  = int(meter_id)
    #         appdata = self.db_obj.find_meter('meters', meter_id)
    #         appdata = dumps(appdata)
    #         appdata = ast.literal_eval(appdata)
    #         if appdata and len(appdata) > 0:
    #             appdata = dumps(appdata)
    #             appdata = ast.literal_eval(appdata)
    #             data = appdata[0]['data']
    #             years = list(data.keys())
    #             active_consumption_for_a_meter = [data[str(i)][str(months)]['loss_likelihood'] for i in years for months in
    #                                               list(data[str(i)].keys())]
    #             npa = np.asarray(active_consumption_for_a_meter, dtype=np.double)
    #             out = str(np.mean(npa))
    #             status = "loss likelihood of"+" "+str(meter_id)+" "+"for all years is"+" "+str(out)
    #         else:
    #             status = "Meter Id does not exist. please enter with proper Meter Id. Thank You"
    #     else:
    #         status = "Meter Id does not exist. please enter with proper Meter Id. Thank You"
    #
    #     return status
    #
    # def loss_likelihood_per_meter_per_Y_M(self,meter_id,year,month):
    #     status = 'Not Availble'
    #     print(meter_id,year,month)
    #     if str(meter_id).isdigit() and len(str(year))==4 and month in range(1,13):
    #         meter_id  = int(meter_id)
    #         appdata = self.db_obj.find_meter_date('meters', meter_id,year,month)
    #         appdata = dumps(appdata)
    #         appdata = ast.literal_eval(appdata)
    #
    #         try:
    #             out = appdata[0]['data'][str(year)][str(month)]['loss_likelihood']
    #             status = "loss likelihood of"+" "+str(meter_id)+" "+"for year" +" " +str(year)+ " "+"and for month"+" "+str(month) +"is"+" "+str(out)
    #         except Exception as e:
    #             status = "Meter Id or year or month does not exist. please enter with proper Meter Id or year or month. Thank You"
    #             self.logger.error(str(e))
    #             exc_type, exc_obj, exc_tb = sys.exc_info()
    #             fname1 = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #             self.logger.error(exc_type, fname1, exc_tb.tb_lineno)
    #             return status
    #
    #     else:
    #         status = "Meter Id or year or month does not exist. please enter with proper Meter Id or year or month. Thank You"
    #
    #     return status






