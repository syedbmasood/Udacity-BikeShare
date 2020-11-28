"""
Code File
"""



# Import required packages


import streamlit as st
import pandas as pd
import statistics
import calendar
import time
import numpy as np
from PIL import Image
import pyodbc
from jinjasql import JinjaSql

###########################################################################################################

def create_query(city):
    """
    Function: This function creats a SQL query in the right format that can successfuly run and fetch results.
    A city name is passed to the function in order to query the approriate table instead of fetching all the data
    at once.
    """
    table = '[dbo].'+ '[' +city +']'
    user_transaction_template = '''
    select
        *
    from
    {{ table_name }}
    '''
    params = {'table_name' : table}

    j = JinjaSql(param_style='pyformat')
    query, bind_params = j.prepare_query(user_transaction_template, params)
    dataq = query % bind_params

    return(dataq)

###########################################################################################################


def get_data(city):
    """
    Function: This function provides connectivity to a SQL Database hosted on Azure and then runs the user
    query. It also calls the create_query function before fetching the results and then returns a dataframe
    back to user.
    """
    
    server = 'tcp:bikeshareapp.database.windows.net'
    database = 'BikeShare'
    username = 'syemas'
    password = 'Khyber354'   
    driver= '{ODBC Driver 17 for SQL Server}'

    connection = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)

    query = create_query(city)
    df = pd.read_sql(query, connection)

    return(df)

###########################################################################################################
 
# Setting the app page with title and a brief description for users

st.title('US Bikeshare Data Exploration') 
st.header('Insights on bikeshare statistics')
st.text('This app allows you to extracts statistics on bikesharing within 3 US cities.')
st.text('You have choices where you can select a city, then a particular month or ')
st.text('all available months, and whole months or particular weekdays within selected month/s.')
image = Image.open('NyBike.jpg')
st.image(image)

###########################################################################################################


# Creating some selection boxes for user choices in streamlit

city = st.sidebar.selectbox('To continue, please select a city',('None','chicago','new_york_city', 'washington'))
month = st.sidebar.selectbox('Please select a month',('None','january', 'february', 'march',' april', 'may',' june'))
day = st.sidebar.selectbox('Please select a day', ('None','sunday','monday','tuesday','wednesday','thursday','friday','saturday'))

###########################################################################################################

def main():

    """
    This functions is run to load data when a user has made a choice for city, month or day and filters the 
    data at the same time before producing statistics
    """
    @st.cache(suppress_st_warning=True)
    def load_data(city, month, day):
        """
        Loads data for the specified city and filters by month and day if applicable.

        Args:
            (str) city - name of the city to analyze
            (str) month - name of the month to filter by, or "all" to apply no month filter
            (str) day - name of the day of week to filter by, or "all" to apply no day filter
        Returns:
            df - Pandas DataFrame containing city data filtered by month and day
        """
        if ((month == 'None') and (day == 'None')):
            ('## The results will be for all available month/s and day/s')
            month = 'all'
            day = 'all'
        
        elif ((month != 'None') and (day == 'None')):
            ('## You selected %s, whole month' %month)
            month = month
            day = 'all'

        elif ((month != 'None') and (day != 'None')):
            ('## You selected %s , and %s' %(month, day))
            month = month
            day = day

        else:
            ('## You selected all available month/s, and %s ' %day)
            month = 'all'
            day = day
    
        df = get_data(city)

        # convert the Start Time column to datetime
        df['Start_Time'] = pd.to_datetime(df['Start_Time']) 

        # extract month and day of week from Start Time to create new columns
        df['month'] = df['Start_Time'].dt.month
        df['day_of_week'] = df['Start_Time'].dt.dayofweek
    
        df['month'] = df['month'].apply(lambda x: calendar.month_name[x].lower())
        df['day_of_week'] = df['day_of_week'].apply(lambda x: calendar.day_name[x].lower())

        df['hour'] = df['Start_Time'].dt.hour
    
        # filter by month if applicable
        if month != 'all':
            # filter by month to create the new dataframe
            df = df[df['month'] == month]

         # filter by day of week if applicable
        if day != 'all':
            # filter by day of week to create the new dataframe
            df = df[df['day_of_week'] == day]
    

        return df

###########################################################################################################

    
    def time_stats(df):
        """Displays statistics on the most frequent times of travel."""

        ('# Calculating The Most Frequent Times of Travel...\n')
        start_time = time.time()

        # TO DO: display the most common month
        mnth = statistics.mode(df['month']).upper()
        day = statistics.mode(df['day_of_week']).upper()
        st_hour = statistics.mode(df['hour'])
        t = time.time() - start_time
        st.text('The most common month is : %s' %mnth)
        st.text('The most common day is : %s' %day)
        st.text('The most common start hour is : %s ' %st_hour)
        st.text('This took : %s seconds' %t)

###########################################################################################################

       
    def station_stats(df):
        """Displays statistics on the most popular stations and trip."""

        ('# Calculating The Most Popular Stations and Trip...')
        start_time = time.time()

        # TO DO: display most commonly used start station
        start_station = statistics.mode(df['Start_Station']).upper()
        end_station = statistics.mode(df['End_Station']).upper()
        
        # TO DO: display most frequent combination of start station and end station trip
        trips = df.groupby(['Start_Station', 'End_Station']).size().reset_index(name='trip_count')
        trips = trips.sort_values('trip_count', ascending = False)
    
        start_station = trips.iloc[0, 0]
        end_station = trips.iloc[0, 1]
        trip_c = trips.iloc[0, 2]
    

        st.text('The most common Start Station was :  %s \n' %start_station)
        st.text('The most common End Station was :  %s   \n' %end_station)
        st.text('The most common combination of Start and End Station was \n\n %s and\n %s' %(start_station, end_station))
        st.text('\nThe total number of trips between the above stations were: %s' %trip_c)
        st.text("\nThis took %s seconds." % (time.time() - start_time))

###########################################################################################################

    
    def trip_duration_stats(df):
        """Displays statistics on the total and average trip duration."""

        ('# Calculating Trip Duration...\n')
        start_time = time.time()

        # TO DO: display total travel time
        total_time = (df['Trip_Duration'].sum())/60
        mean_time = (df['Trip_Duration'].mean())/60

        st.text('The total travel time in minutes was:  %s ' %total_time)
        st.text('The average travel time in minutes was: %s ' %mean_time )
        st.text("\nThis took %s seconds." % (time.time() - start_time))
       
###########################################################################################################

    if(city =='None'):
        ('## Please select a city to continue!')
    else:
        ('# You have selected the following options ')
        ('## Your selected city is: %s' %city)
    
        data = load_data(city, month, day)

        if st.button('Time Statistics'):
            st.write(time_stats(data))

        if st.button(' Station Statistics'):
            st.write(station_stats(data))

        if st.button('Trip Duration'):
            st.write(trip_duration_stats(data))
   
        if st.button('Reset'):
            ('Clearning Stats')

###########################################################################################################

if __name__ == "__main__":
    main()