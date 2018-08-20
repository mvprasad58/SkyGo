
from rest_framework.response import Response
from rest_framework.views import APIView

from django.http import HttpResponse
import json
from chatbotapp.mathoperations import math_op
import gridfs
from pymongo import MongoClient
from bson.json_util import dumps
import ast
from chatbotapp.db import databaseOperations as db
import numpy as np
from chatbotapp.functionalities import chat_functionalities
from chatbotapp.db import skygodb
import datetime
import re

class chatbot(APIView):

    def post(self, request):
        chat_funct_obj = chat_functionalities()
        # var1 = str(request.data['queryResult']['action'])
        displayname = str(request.data['queryResult']['intent']['displayName'])

        out = "Intent is "


        if displayname == 'MActiveConsumption':
            meter_id = request.data['queryResult']['parameters']['meter']
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.MactiveConsumption(meter_id,year,month)
        elif displayname == 'MActiveConsumptionC':
            meter_id = request.data['queryResult']['parameters']['meter']
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.MactiveConsumption(meter_id,year,month)

        elif displayname == 'MAgeofMeter':
            meter_id = request.data['queryResult']['parameters']['meter']
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.MAgeofMeter(meter_id,year,month)

        elif displayname == 'MAgeofMeterC':

            meter_id = request.data['queryResult']['parameters']['meter']
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.MAgeofMeter(meter_id,year,month)

        elif displayname == "MCalcLossLikelihood":

            meter_id = request.data['queryResult']['parameters']['meter']
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.MCalcLossLikelihood(meter_id,year,month)

        elif displayname == "MCalcLossLikelihoodC":

            meter_id = request.data['queryResult']['parameters']['meter']
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.MCalcLossLikelihood(meter_id,year,month)

        elif displayname == "MReactiveConsumption":

            meter_id = request.data['queryResult']['parameters']['meter']
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.MReactiveConsumption(meter_id,year,month)

        elif displayname == "MReactiveConsumptionC":

            meter_id = request.data['queryResult']['parameters']['meter']
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.MReactiveConsumption(meter_id,year,month)

        elif displayname == "MStdDeviation":

            meter_id = request.data['queryResult']['parameters']['meter']
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.MStdDeviation(meter_id,year,month)

        elif displayname == "MStdDeviationC":

            meter_id = request.data['queryResult']['parameters']['meter']
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.MStdDeviation(meter_id,year,month)

        elif displayname == "MTotConsumption":

            meter_id = request.data['queryResult']['parameters']['meter']
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.MTotConsumption(meter_id,year,month)

        elif displayname == "MTotConsumptionC":

            meter_id = request.data['queryResult']['parameters']['meter']
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.MTotConsumption(meter_id,year,month)

        elif displayname == "SActCons":

            substation = request.data['queryResult']['parameters']['substation']
            # substation = substation.upper()
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.SActCons(substation,year,month)

        elif displayname == "SActConsC":

            substation = request.data['queryResult']['parameters']['substation']
            # substation = substation.upper()
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.SActCons(substation,year,month)

        elif displayname == "SConsumption":

            substation = request.data['queryResult']['parameters']['substation']
            # substation = substation.upper()
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.SConsumption(substation, year, month)

        elif displayname == "SConsumptionC":

            substation = request.data['queryResult']['parameters']['substation']
            # substation = substation.upper()
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.SConsumption(substation, year, month)

        elif displayname == 'SLossLikelihood':

            substation = request.data['queryResult']['parameters']['substation']
            # substation = substation.upper()
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.SLossLikelihood(substation, year, month)


        elif displayname == 'SLossLikelihoodC':

            substation = request.data['queryResult']['parameters']['substation']
            # substation = substation.upper()
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.SLossLikelihood(substation, year, month)

        elif displayname == 'SReactCons':

            substation = request.data['queryResult']['parameters']['substation']
            # substation = substation.upper()
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.SReactCons(substation, year, month)


        elif displayname == 'SReactConsC':

            substation = request.data['queryResult']['parameters']['substation']
            # substation = substation.upper()
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.SReactCons(substation, year, month)

        elif displayname == 'STransConnected':

            substation = request.data['queryResult']['parameters']['substation']
            # substation = substation.upper()
            #
            # date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            # date1 = date_period.split("T")[0].split("-")
            # year = date1[0]
            # month = int(date1[1])
            out = chat_funct_obj.STransConnected(substation)

        elif displayname == 'STransConnectedC':

            substation = request.data['queryResult']['parameters']['substation']
            # substation = substation.upper()
            #
            # date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            # date1 = date_period.split("T")[0].split("-")
            # year = date1[0]
            # month = int(date1[1])
            out = chat_funct_obj.STransConnected(substation)

        elif displayname == 'STransConsumption':

            substation = request.data['queryResult']['parameters']['substation']
            # substation = substation.upper()
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.STransConsumption(substation, year, month)

        elif displayname == 'STransConsumptionC':

            substation = request.data['queryResult']['parameters']['substation']
            # substation = substation.upper()
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.STransConsumption(substation, year, month)


        elif displayname == 'TActiveCons':

            transformer = request.data['queryResult']['parameters']['transformer']

            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.TActiveCons(transformer, year, month)

        elif displayname == 'TActiveConsC':

            transformer = request.data['queryResult']['parameters']['transformer']

            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.TActiveCons(transformer, year, month)

        elif displayname == 'TLossLikelihood':

            transformer = request.data['queryResult']['parameters']['transformer']

            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.TLossLikelihood(transformer, year, month)

        elif displayname == 'TLossLikelihoodC':

            transformer = request.data['queryResult']['parameters']['transformer']

            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.TLossLikelihood(transformer, year, month)

        elif displayname == 'TLossPercentage':

            transformer = request.data['queryResult']['parameters']['transformer']

            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.TLossPercentage(transformer, year, month)

        elif displayname == 'TLossPercentageC':

            transformer = request.data['queryResult']['parameters']['transformer']

            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.TLossPercentage(transformer, year, month)

        elif displayname == 'TMeterCons':

            transformer = request.data['queryResult']['parameters']['transformer']

            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.TMeterCons(transformer, year, month)

        elif displayname == 'TMeterConsC':

            transformer = request.data['queryResult']['parameters']['transformer']

            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.TMeterCons(transformer, year, month)

        elif displayname == 'TMetersConnected':

            transformer = request.data['queryResult']['parameters']['transformer']
            transformer = int(transformer)
            # substation = substation.upper()
            #
            # date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            # date1 = date_period.split("T")[0].split("-")
            # year = date1[0]
            # month = int(date1[1])

            if transformer == 8530:
                # out = "The 2 meters connected to the given transformer are, 2216, 2179"
                out = "Los 2 metros conectados al transformador dado son, " + "2216, 2179" + "."
            else:
                out = chat_funct_obj.TMetersConnected(transformer)

        elif displayname == 'TMetersConnectedC':

            transformer = request.data['queryResult']['parameters']['transformer']
            # substation = substation.upper()
            #
            # date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            # date1 = date_period.split("T")[0].split("-")
            # year = date1[0]
            # month = int(date1[1])
            out = chat_funct_obj.TMetersConnected(transformer)

        elif displayname == 'TReactiveCons':

            transformer = request.data['queryResult']['parameters']['transformer']

            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.TReactiveCons(transformer, year, month)

        elif displayname == 'TReactiveConsC':

            transformer = request.data['queryResult']['parameters']['transformer']

            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.TReactiveCons(transformer, year, month)

        elif displayname == 'TStdDeviation':

            transformer = request.data['queryResult']['parameters']['transformer']

            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.TStdDeviation(transformer, year, month)

        elif displayname == 'TStdDeviationC':

            transformer = request.data['queryResult']['parameters']['transformer']

            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.TStdDeviation(transformer, year, month)

        elif displayname == 'TTotConsumption':

            transformer = request.data['queryResult']['parameters']['transformer']

            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.TTotConsumption(transformer, year, month)

        elif displayname == 'TTotConsumptionC':

            transformer = request.data['queryResult']['parameters']['transformer']

            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.TTotConsumption(transformer, year, month)

        elif displayname == 'average_loss_all_years':
            print(displayname)
            meter_id = request.data['queryResult']['parameters']['meter_id']
            # zone = request.data['queryResult']['parameters']['zone']
            meter_parameter = request.data['queryResult']['parameters']['meter_parameters']
            out = chat_funct_obj.average_loss_likeli_per_zone_all_years(meter_id, meter_parameter)


        elif displayname == 'average_loss_all_yearsC':
            meter_id = request.data['queryResult']['parameters']['meter_id']
            # zone = request.data['queryResult']['parameters']['zone']
            meter_parameter = request.data['queryResult']['parameters']['meter_parameters']
            date_period = request.data['queryResult']['parameters']['dateperiod']['endDate']
            date1 = date_period.split("T")[0].split("-")
            year = date1[0]
            month = int(date1[1])
            out = chat_funct_obj.average_loss_all_yearsC(meter_id, meter_parameter,year)

        elif displayname == 'more_losses_in_a_substation':
            substation = request.data['queryResult']['parameters']['substation']
            out = chat_funct_obj.more_losses_in_a_substation(substation)

        elif displayname == 'active_consumption_all_years':
            meter = request.data['queryResult']['parameters']['meter']
            out = chat_funct_obj.active_consumption_all_years(meter)

        else:
            pass




        # if var1 == 'active_consumption_all_years':
        #     meter_id = request.data['queryResult']['parameters']['meter_id']
        #
        #     out = chat_funct_obj.active_consumption_all_years(meter_id)
        #
        # elif var1 == 'active_cons_per_meter_per_Y_M':
        #
        #     meter_id = request.data['queryResult']['parameters']['meter_id']
        #     date_period = request.data['queryResult']['parameters']['date-period']['startDate']
        #
        #     date1 = date_period.split("T")[0].split("-")
        #     year = date1[0]
        #     month = int(date1[1])
        #     out = chat_funct_obj.active_cons_per_meter_per_Y_M(meter_id,year,month)
        #
        # elif var1 == 'loss_likelihood_all_years':
        #
        #     meter_id = request.data['queryResult']['parameters']['meter_id']
        #     out = chat_funct_obj.loss_likelihood_all_years(meter_id)
        # elif var1 == 'loss_likelihood_per_meter_per_Y_M':
        #
        #     meter_id = request.data['queryResult']['parameters']['meter_id']
        #     date_period = request.data['queryResult']['parameters']['date-period']['startDate']
        #
        #     date1 = date_period.split("T")[0].split("-")
        #     year = date1[0]
        #     month = int(date1[1])
        #     out = chat_funct_obj.loss_likelihood_per_meter_per_Y_M(meter_id, year, month)
        #
        # elif var1 == 'average_loss_likeli_per_zone_all_years':
        #
        #     meter_id = request.data['queryResult']['parameters']['meter_id']
        #     zone = request.data['queryResult']['parameters']['zone']
        #     meter_parameter = request.data['queryResult']['parameters']['meter_parameters']
        #     out = chat_funct_obj.average_loss_likeli_per_zone_all_years(meter_id, zone,meter_parameter)
        #
        #
        # else:
        #     out = "query is not matched with any of the intents trained"

            # appdata = db_obj.find_meter('meters',meter_id)
            # if (appdata != None):
            #     appdata = dumps(appdata)
            #     appdata = ast.literal_eval(appdata)
            #     print(appdata)
            #     data = appdata[0]['data']
            #     years = list(data.keys())
            #     active_consumption_for_a_meter = [data[str(i)][str(months)]['active_consum'] for i in years for months in list(data[str(i)].keys())]
            #     npa = np.asarray(active_consumption_for_a_meter, dtype=np.double)
            #     out =  str(np.mean(npa))
            #
            # else:
            #     out =  "Nothing Found"

        response = {'fulfillmentText': out}
        response = json.dumps(response)
        return HttpResponse(response)


class skygo(APIView):

    def post(self, request):
        chat_funct_obj = skygodb()
        # var1 = str(request.data['queryResult']['action'])
        displayname = str(request.data['queryResult']['intent']['displayName'])

        out = "Intent is not matched"
        message = ""

        if displayname == 'Departure':
            unique_carrier = request.data['queryResult']['parameters']['unique_carrier']
            flightnumber = request.data['queryResult']['parameters']['flightnumber']
            ti = chat_funct_obj.departure(unique_carrier,flightnumber)
            out = "Flightnumber " + str(unique_carrier) + "-" + str(
                flightnumber) + " is now scheduled to depart at " + str(
                ti) + " as compared to the earlier departure time of 18:30"
        elif displayname == 'DepartureC':
            unique_carrier = request.data['queryResult']['parameters']['unique_carrier']
            flightnumber = request.data['queryResult']['parameters']['flightnumber']
            ti = chat_funct_obj.departure(unique_carrier,flightnumber)
            out = "Flightnumber " + str(unique_carrier) + "-" + str(flightnumber) + " is now scheduled to depart at " + str(
                ti) + " as compared to the earlier departure time of 18:30"

        elif displayname == 'FlightStatus':
            unique_carrier = request.data['queryResult']['parameters']['unique_carrier']
            flightnumber = request.data['queryResult']['parameters']['flightnumber']
            duration,deptime,arrtime = chat_funct_obj.FlightStatus(unique_carrier,flightnumber)
            out = "I think, you were referring to "+str(unique_carrier)+str(flightnumber)+". "+str(unique_carrier)+str(flightnumber)+" is scheduled to leave Indianapolis International Airport at "+str(deptime)+" and arrive at the Orlando International airport at"+ str(arrtime)+" with a flight duration of "+str(duration)+" hours."

        elif displayname == 'FlightStatusC':
            unique_carrier = request.data['queryResult']['parameters']['unique_carrier']
            flightnumber = request.data['queryResult']['parameters']['flightnumber']
            duration,deptime,arrtime = chat_funct_obj.FlightStatus(unique_carrier,flightnumber)
            out = "I think, you were referring to "+str(unique_carrier)+str(flightnumber)+". "+str(unique_carrier)+str(flightnumber)+" is scheduled to leave Indianapolis International Airport at "+str(deptime)+" and arrive at the Orlando International airport at"+ str(arrtime)+" with a flight duration of "+str(duration)+" hours."

        elif displayname == 'Thankyou':
            hour = int(datetime.datetime.time(datetime.datetime.now()).hour)
            minute = int(datetime.datetime.time(datetime.datetime.now()).minute)
            if hour in range(12,19):
                out = "Its "+str(hour)+"PM"+ " in the afternoon, do you want to have lunch before you go? I can give you some recommendations."
            elif hour in range(5,12):
                out = "Its " + str(hour) + str("AM")+ " in the morning, do you want to have breakfast before you go? I can give you some recommendations."
            elif hour in range(19,25):
                out = "Its " + str(hour)+str("PM")+" in the night, do you want to have dinner before you go? I can give you some recommendations."
            elif hour in range(1,5):
                out = "Its " + str(hour)+ "AM"+" in the early morning, do you want to have any food before you go? I can give you some recommendations."
            else:
                pass

        elif displayname == 'Food_Restaurant':
            cuisine = request.data['queryResult']['parameters']['cuisine']
            out = chat_funct_obj.Food_Restaurant(cuisine)
            message = {"platform": "ACTIONS_ON_GOOGLE","image": {"imageUri": "https://images.pexels.com/photos/262978/pexels-photo-262978.jpeg?auto=compress&cs=tinysrgb&h=350"}

}
            response = {'fulfillmentText': out,"fulfillmentMessages":[message]}
            response = json.dumps(response)
            return HttpResponse(response)

        elif displayname == 'Find_Restaurant':
            restaurant = request.data['queryResult']['parameters']['restaurant']
            out = chat_funct_obj.Find_Restaurant(restaurant)


        elif displayname == 'Food_rating':
            restaurant = request.data['queryResult']['parameters']['restaurant']
            out = chat_funct_obj.Food_rating(restaurant)

        elif displayname == 'Food_ratingC':
            restaurant = request.data['queryResult']['parameters']['restaurant']
            out = chat_funct_obj.Food_rating(restaurant)

        elif displayname == 'FlightCancellation':
            flightnumber = str(request.data['queryResult']['parameters']['flightnumber'])
            unique_carrier = request.data['queryResult']['parameters']['unique_carrier']
            # print(flightnumber)
            # flightnumber1 = int(re.findall('\d+', flightnumber )[0])
            # unique_carrier = re.findall(r"(?i)\b[a-z]+\b", flightnumber)[0]
            out = chat_funct_obj.FlightCancellation(flightnumber,unique_carrier)

        elif displayname == 'depart_gate_hindi':
            flightnumber = request.data['queryResult']['parameters']['flightnumber']
            unique_carrier = request.data['queryResult']['parameters']['unique_carrier']
            out = chat_funct_obj.depart_gate_hindi(unique_carrier,flightnumber)
        else:
            pass

        response = {'fulfillmentText': out}
        response = json.dumps(response)
        return HttpResponse(response)





