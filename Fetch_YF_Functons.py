import yfinance as yf
import datetime
import time

from datetime import date
from datetime import datetime, date, timedelta
from datetime import datetime
from zeroKey import *




def count_date_Diff (start_Date, end_Date) :

    """ 
        This Function count deferent interval between start to end date
        The output is integer and only Date 
    """
    
   
    if str(type(end_Date))  == "<class 'datetime.datetime'>" :
        end_Date = (str(datetime.now())[0:10])
        print("yess")

    startDate = date(int(start_Date[0:4]) , int(start_Date[5:7]),  int(start_Date[8:10]))
    endDate = date(int(end_Date[0:4]) , int(end_Date[5:7]),  int(end_Date[8:10]))
    intervalDate = endDate - startDate  
    end_Now = end = (str(datetime.now())[0:10])
    end_wanted = date(int(end_Now[0:4]) , int(end_Now[5:7]),  int(end_Now[8:10]))
    interval_from_Now = end_wanted - startDate

    print("intervalDate      : ", intervalDate.days)
    print("interval_from_Now : ", interval_from_Now.days)
   
   
    return int(intervalDate.days) , int(interval_from_Now.days)  # count enddate - start date  ,  start Day for find last days




def tryExcept_Offline(strTicker, strStart_Date, strEnd_Date, strInterval) :
     
     
     try:
            data = yf.download(strTicker, strStart_Date, strEnd_Date, interval=strInterval)
            if data.empty:  
                        time.sleep(5)
                        print( "\nData is Empty.I'm Tring again...  ", strInterval)
                        frame = data.to_sql(con=database_connection, name=table_name , if_exists='replace')

     except :
             print("Except  : --------------------------------------------------" , )
             print("\n\n May be Download is Faild . I'm trying again ... ")
             errorCounter = 0
             _checkEmpty = True
             while _checkEmpty :
                                errorCounter +=1
                                print("\nErrorCounter :", errorCounter)
                                data = yf.download(strTicker, strStart_Date, strEnd_Date, interval=strInterval)
                                print("startDate: ", strStart_Date, "endDate : ", strEnd_Date)
                                if data.empty:  
                                        time.sleep(5)
                                        print( "\nData is Empty.I'm Tring again...  ", strInterval)
                                else:
                                         _checkEmpty= False
                                         table_name="{ticker}_{interval}".format(ticker= strTicker.replace("-","") ,interval= strInterval)
                                         time.sleep(1)
     
     return data

def fetch_DataF(strTicker, strStart_Date, strEnd_Date, strInterval) :

    """
        This Function make fetch data from yahoo finance according to ticker, start and end date and timeframe(strinterval)
        output : if everything is normal is a data table
        output : if timeframe(strinterval) be incorrecy is "-1"
    """
    
    checkin = False

    if str(type(strEnd_Date))  == "<class 'datetime.datetime'>" :
            strEnd_Date = (str(datetime.now())[0:10])
            print("---------- > End_Date = datetime.now \n\n")

    print("\n\n\nStart_Date : " , strStart_Date, "\nEnd_Date   : ", strEnd_Date, "\n\n")
    intervalDate, interval_from_Now = (count_date_Diff(str(strStart_Date), strEnd_Date))


    if (strInterval == "1m") :
        
        if intervalDate < 31 and interval_from_Now < 8 :

            print("\nStart_Date        : ", strStart_Date)
            print("End_Date          : ", strEnd_Date)
            print("interval          : ", strInterval ,"\n\n")

            data = tryExcept_Offline(strTicker, strStart_Date, strEnd_Date, strInterval)
            
        
        else:
        
            delta = interval_from_Now - 29
            strStart_Date = str((datetime.strptime(strStart_Date, '%Y-%m-%d') + timedelta(days=delta)).date())
            print("\n -------------- > Forced new start date: ", strStart_Date)
            print(" -------------- >            end   date: ", strEnd_Date , "\n")
            intervalDate, interval_from_Now = (count_date_Diff(str(strStart_Date), strEnd_Date))
        

            if intervalDate > 8 :
                
                decide = input("\n\"intervalDate > 8\"\nInterval between Start_Date and End_Date is more than 7 days, please choose constant:(start/end) : ")
                delta = intervalDate - 6
                
                if decide == "start" :

                    strEnd_Date = str((datetime.strptime(strEnd_Date, '%Y-%m-%d') - timedelta(days=delta)).date())
                    
                    print("\nStart_Date        : ", strStart_Date)
                    print("End_Date          : ", strEnd_Date)
                    print("interval          : ", strInterval ,"\n\n")

                    data = tryExcept_Offline(strTicker, strStart_Date, strEnd_Date, strInterval)
                    

                if decide == "end" :

                    strStart_Date = str((datetime.strptime(str(strStart_Date), '%Y-%m-%d') + timedelta(days=delta)).date())

                    print("\nStart_Date        : ", strStart_Date)
                    print("End_Date          : ", strEnd_Date)
                    print("interval          : ", strInterval ,"\n\n")

                    data = tryExcept_Offline(strTicker, strStart_Date, strEnd_Date, strInterval)
                    


    elif (strInterval == "2m") or (strInterval == "5m") or  (strInterval == "15m") or (strInterval == "30m") :
        
        if interval_from_Now < 60 :
            
            print("\nStart_Date        : ", strStart_Date)
            print("End_Date          : ", strEnd_Date)
            print("interval          : ", strInterval ,"\n\n")

            data = tryExcept_Offline(strTicker, strStart_Date, strEnd_Date, strInterval)

        else:
        
            delta = interval_from_Now - 59
            strStart_Date = str((datetime.strptime(strStart_Date, '%Y-%m-%d') + timedelta(days=delta)).date())

            print("\nStart_Date        : ", strStart_Date)
            print("End_Date          : ", strEnd_Date)
            print("interval          : ", strInterval ,"\n\n")

            data = tryExcept_Offline(strTicker, strStart_Date, strEnd_Date, strInterval)
            
            

    elif (strInterval == "60m") or (strInterval == "1h"):

        if interval_from_Now < 729 :
            
            print("\nStart_Date        : ", strStart_Date)
            print("End_Date          : ", strEnd_Date)
            print("interval          : ", strInterval ,"\n\n")

            data = tryExcept_Offline(strTicker, strStart_Date, strEnd_Date, strInterval)
            

        else:
        
            delta = interval_from_Now - 729
            strStart_Date = str((datetime.strptime(strStart_Date, '%Y-%m-%d') + timedelta(days=delta)).date())

            print("\nStart_Date        : ", strStart_Date)
            print("End_Date          : ", strEnd_Date)
            print("interval          : ", strInterval ,"\n\n")

            data = tryExcept_Offline(strTicker, strStart_Date, strEnd_Date, strInterval)
            
            

    elif (strInterval == "1d") or (strInterval == "5d")or (strInterval == "1wk")or (strInterval == "1mo")or (strInterval == "3mo"):
        
        
            print("\nStart_Date        : ", strStart_Date)
            print("End_Date          : ", strEnd_Date)
            print("interval          : ", strInterval ,"\n\n")

            data = tryExcept_Offline(strTicker, strStart_Date, strEnd_Date, strInterval)

            
    else:
        
        checkin = True
    

    return data if checkin == False else (print("\n\n\n....> Fetch Data failed because ticker or interval are incorrect !\n\n\n\n"))