import os, pickle, argparse, csv, requests, datetime,time


# Initialize parser
parser = argparse.ArgumentParser(
     description="Collect a data export for concentrator"
)

url_template = "http://<HOST>:<PORT>/sdk/?msg=values&force-content-type=text/plain&where=<WHERE_CLAUSE>time='<FIRST_TIME>'-'<LAST_TIME>'&fieldName=<FIELD_NAME>&size=<SIZE>&flags=order-ascending%2Csort-value%2C<CATEGORY>"


# Set the username and password for authentication
username = '********'
password = '**********'

meta_keys = ['device.type', 'device.host', "device.ip", "org"]
timeframes = ["daily", "hourly", "minute"]
how_long_ago = ['1d', '2d', '4d', "7d", '30d', 'custom_date']
categories=['size', 'sessions']
months=["Jan", "Feb", "Mar", "Apr", 'May', "June", 'July', "Aug", "Sep", "Oct", "Dec"]
years=['2023']
SIZE=1000
CONCENTRATORS=["192.168.33.30","192.168.33.31", "192.168.33.32", "172.19.119.201", "192.168.33.33", "192.168.33.34"]
PORT="50105" #Port Number. Concentrator: 50105, Broker: 50103 
cwd = f"{os.getcwd()}/"
sleep_time = 60

class VolumeCollector():
    def __init__(self, **kwargs):
        self.filter_ = kwargs["filter_"]
        self.timeframe = kwargs["timeframe"]
        self.fieldname = kwargs['fieldname']
        self.url_template = kwargs['url_template']
        self.port = kwargs['port']
        self.hosts = kwargs['hosts']
        self.categories = kwargs['categories']
        self.time_ago = kwargs['time_ago']
        self.start_date = kwargs['start_date']
        self.end_date = kwargs['end_date']
        self.filter_key=kwargs['filter_key']
        self.size=kwargs['size']

        # build query
        self.url = self.build_query()


        # Collect data
        self.data = self.collect_data()
        
        print(f"Collected data: {self.data}")

        # Save this data in a json format file
        # send csv to var log file
        with open(f"log_volumes{self.fieldname}s.pickle", 'wb') as pickle_out:
            pickle.dump(self.data, pickle_out)
        self.save_data()
    
    def get_ctr(self, **kwargs):
        host = kwargs['ctr']
        i = kwargs['i']
        crt = f"orginationctr0{str(i)}"

        if host == '192.168.33.30':
            crt = "orginationctr01"

        elif host == "192.168.33.31":
            crt = "orginationctr02"

        elif host == "192.168.33.32":
            crt = "orginationctr03"

        elif host == "192.168.33.33":
            crt = 'orginationctr04'

        elif host == '192.168.33.34':
            crt = 'orginationctr05'

        elif host == '172.19.119.201':
            crt = 'organisarionrsavm02' 

        return crt

    def save_data(self, **kwargs):
        # Fields both file types
        byte_fields = ["bytes","ip","datetime", "org", "concentrator"]
        count_fields = ["count", "ip", "datetime", "org", "concentrator"]

        data = self.data

        for filt, val in data.items():
            # print(f"Filter: {filt}")
            i=1
            for concen, value in data[filt].items():

                for evnt_type, volumes in value.items():

                    if evnt_type == 'byte_count':
                        fields=byte_fields
                    elif evnt_type == 'event_count':
                        fields=count_fields

                    filename1 = f'data/Concentrator{i}_{filt}_data_{evnt_type}.csv'

                    prefix = self.fieldname.replace(".", "_")
                    filename = f'/var/log/nw_api_export/{prefix}_volumes_data_{evnt_type}.csv'
                    
                    # Update concentrator name according to the IP
                    ctr = self.get_ctr(ctr=concen)
 
                    # add additional field
                    volumes = self.add_fields(concentrator=f'MTGTCOCTR0{i}', org=filt, data=volumes)
                    
                    # writing to csv file
                    for vol in volumes: 
                        with open(filename, 'a') as csvfile: 
                            # creating a csv writer object 
                            csvwriter = csv.writer(csvfile) 
        
                            # writing the data rows 
                            csvwriter.writerows([vol])
                i=i+1
                print(f'\n\nKey: {concen}\nVal: {value}')
    
    def add_fields(self, **kwargs):
        #Get required data 
        concentrator = kwargs['concentrator']
        org = kwargs['org']
        data = kwargs['data']
       
        print(f'data: {data}\nConcentrator{concentrator}\norg:{org}')
        for i in range(len(data)):
            row = data[i]
            row.append(concentrator)
            row.append(org)

            data[i] = row
            
        return data 

    def collect_data(self, **kwargs):

        # Loop through each where clause
        all_data = {}
        if self.filter_:
            pass
        else:
            self.filter_ = [""]

        for filt in self.filter_:
            if filt == "":
                none = ""
                filt='No filter'
                all_data[filt] = {} 
                new_url_ = self.url.replace("<WHERE_CLAUSE>", "")
            else:
                all_data[filt] = {}
                where_clause = f"{self.filter_key}='{filt}'+AND+"
                new_url_ = self.url.replace("<WHERE_CLAUSE>", where_clause)

            # Loop through all host manipulating the url as needed
            for host in self.hosts:
                all_data[filt][host] = {}

                # Fix host in url
                new_url = new_url_.replace("<HOST>", host)

                # Fix data
                if self.timeframe == 'daily':
                    data_bytes, data_count = self.collect_daily(url=new_url)
                    if data_bytes != []: 
                        all_data[filt][host]['byte_count'] = data_bytes
                        all_data[filt][host]['event_count'] = data_count

                elif self.timeframe == 'hourly':
                    data_bytes, data_count = self.collect_hourly(url=new_url)

                    if data_bytes != []:
                        all_data[filt][host]['byte_count'] = data_bytes
                        all_data[filt][host]['event_count'] = data_count
                    

        return all_data

    def collect_hourly(self, **kwargs):
        new_url = kwargs['url']
        data_bytes = []
        data_count = []

        if self.time_ago == "custom_date":
            #Prepare date for collection
            start_year = int(self.start_date.split('-')[0])
            start_month = int(self.start_date.split('-')[1])
            start_day = int(self.start_date.split('-')[2])
            end_year = int(self.end_date.split('-')[0])
            end_month = int(self.end_date.split('-')[1])
            end_day = int(self.end_date.split('-')[2])

            x = datetime.datetime(start_year,start_month,start_day)
            start_date = x.strftime('%Y-%B-%d')

            y = datetime.datetime(end_year,end_month,end_day)
            end_date = y.strftime('%Y-%B-%d')

            time_delta = (y-x).days
            if time_delta >= 1:
                for i in range(0,time_delta):
                   for j in range(0,24):
                        new_date = x+datetime.timedelta(days=i)
                        format_="%Y-%B-%d %H:%M:%S"
    
                        if new_date < datetime.datetime.strptime(f"{end_date} 23:59:59", format_):
                            if j <= 9:
                                first_hour = f'0{str(j)}'
                            else:
                                first_hour = str(j)
                            

                            if ((j) <= 9):
                                last_hour = f'0{str(j)}'
                            else:
                                last_hour = str(j)


                            strt_date = f"{new_date.strftime('%Y-%B-%d')}+{first_hour}:00:00"
                            ed_date = f"{new_date.strftime('%Y-%B-%d')}+{last_hour}:59:59"

                            # Fix url timespan
                            download_start_date = datetime.datetime.strptime(strt_date, "%Y-%B-%d+%H:%M:%S") - datetime.timedelta(hours=2)
                            download_end_date = datetime.datetime.strptime(ed_date, "%Y-%B-%d+%H:%M:%S") - datetime.timedelta(hours=2)
                            url = new_url.replace('<FIRST_TIME>', str(download_start_date))
                            url = url.replace('<LAST_TIME>', str(download_end_date))


                            print(f'{url}')

                            # Loop through and get the categories
                            for cat in self.categories:
                                cat_url = url.replace("<CATEGORY>", cat)
    
                                # Loop through and Download data for both bytes and count
                                data_list = self.download_data(url=cat_url, start_day=strt_date, category=cat)

                                if cat == "size":
                                    # load list
                                    for evnt in data_list:
                                        data_bytes.append(evnt)
                                else:
                                    for evnt in data_list:
                                        data_count.append(evnt)
            else:
                # Update end date
                end_date = y+datetime.timedelta(days=1)
                end_date = end_date+datetime.timedelta(hours=23)
                end_date = end_date+datetime.timedelta(minutes=59)
                end_date = end_date.strftime('%Y-%B-%d')
                for i in range(0,1):
                    for j in range(0,24):
                        new_date = x+datetime.timedelta(days=i)
                        format_="%Y-%B-%d %H:%M:%S"
                        if new_date < datetime.datetime.strptime(f"{end_date} 00:00:00", format_):
                            if j <= 9:
                                first_hour = f'0{str(j)}'
                            else:
                                first_hour = str(j)


                            if ((j) <= 9):
                                last_hour = f'0{str(j)}'
                            else:
                                last_hour = str(j)


                            strt_date = f"{new_date.strftime('%Y-%B-%d')}+{first_hour}:00:00"
                            ed_date = f"{new_date.strftime('%Y-%B-%d')}+{last_hour}:59:59"

                            # Fix url timespan
                            download_start_date = datetime.datetime.strptime(strt_date, "%Y-%B-%d+%H:%M:%S") - datetime.timedelta(hours=2)
                            download_end_date = datetime.datetime.strptime(ed_date, "%Y-%B-%d+%H:%M:%S") - datetime.timedelta(hours=2)
                            url = new_url.replace('<FIRST_TIME>', str(download_start_date))
                            url = url.replace('<LAST_TIME>', str(download_end_date))


                            print(f'{url}')

                            # Loop through and get the categories
                            for cat in self.categories:
                                cat_url = url.replace("<CATEGORY>", cat)
                                
                                # Loop through and Download data for both bytes and count
                                data_list = self.download_data(url=cat_url, start_day=strt_date, category=cat)

                                if cat == "size":
                                # load list
                                    for evnt in data_list:
                                        data_bytes.append(evnt)
                                else:
                                    for evnt in data_list:
                                        data_count.append(evnt)
        elif 'd' in self.time_ago:
            past_days = int(self.time_ago.replace('d', ""))

            for i in range(0,past_days):

                for j in range(0, 24):
                    # Prepare date for data collection
                    DAY = str(-past_days + i)
                    date_obj = datetime.datetime.now()
                    new_date = date_obj + datetime.timedelta(days=eval(DAY))

                    # Edit if needed to fix the date
                    if j <= 9:
                        start_day = f"{new_date.strftime('%Y-%B-%d')}+0{str(j)}:00:00"
                    else:
                        start_day = f"{new_date.strftime('%Y-%B-%d')}+{str(j)}:00:00"
                    end_day = f"{new_date.strftime('%Y-%B-%d')}+23:59:59"

                    # Fix url timespan
                    url = new_url.replace('<FIRST_TIME>', start_day)
                    url = url.replace('<LAST_TIME>', end_day)

                    for cat in self.categories:
                        cat_url = url.replace("<CATEGORY>", cat)

                        # Loop through and Download data for both bytes and count
                        data_list = self.download_data(url=cat_url, start_day=start_day,category=cat)

                        if cat == "size":
                            # load list
                            for evnt in data_list:
                                data_bytes.append(evnt)
                        else:
                            for evnt in data_list:
                                data_count.append(evnt)


        return data_bytes, data_count

    def collect_daily(self, **kwargs):
        new_url = kwargs['url']
        data_bytes = []
        data_count = []
        print("we in") 
        if self.time_ago == "custom_date":
            #Prepare date for collection
            start_year = int(self.start_date.split('-')[0])
            start_month = int(self.start_date.split('-')[1])
            start_day = int(self.start_date.split('-')[2])
            end_year = int(self.end_date.split('-')[0])
            end_month = int(self.end_date.split('-')[1])
            end_day = int(self.end_date.split('-')[2])
            x = datetime.datetime(start_year,start_month,start_day)
            start_date = x.strftime('%Y-%B-%d')

            y = datetime.datetime(end_year,end_month,end_day)
            end_date = y.strftime('%Y-%B-%d')

            time_delta = (y-x).days
            if time_delta >= 1:
                for i in range(0,time_delta):
                    new_date = x+datetime.timedelta(days=i)
                    format_="%Y-%B-%d %H:%M:%S"

                    if new_date < datetime.datetime.strptime(f"{end_date} 23:59:59", format_):
                        strt_date = f"{new_date.strftime('%Y-%B-%d')}+00:00:00"
                        ed_date = f"{new_date.strftime('%Y-%B-%d')}+23:59:59"

                        # Fix url timespan
                        url = new_url.replace('<FIRST_TIME>', strt_date)
                        url = url.replace('<LAST_TIME>', ed_date)


                        print(f'{url}')

                        # Loop through and get the categories
                        for cat in self.categories:
                            cat_url = url.replace("<CATEGORY>", cat)

                            # Loop through and Download data for both bytes and count
                            data_list = self.download_data(url=cat_url, start_day=strt_date, category=cat)

                            if cat == "size":
                                # load list
                                for evnt in data_list:
                                    data_bytes.append(evnt)
                            else:
                                for evnt in data_list:
                                    data_count.append(evnt)
            else:
                for i in range(0,1):
                    new_date = x+datetime.timedelta(days=i)

                    format_="%Y-%B-%d %H:%M:%S"
                    if new_date < datetime.datetime.strptime(f"{end_date} 23:59:59", format_):
                        strt_date = f"{new_date.strftime('%Y-%B-%d')}+00:00:00"
                        ed_date = f"{new_date.strftime('%Y-%B-%d')}+23:59:59"

                        # Fix url timespan
                        url = new_url.replace('<FIRST_TIME>', strt_date)
                        url = url.replace('<LAST_TIME>', ed_date)



                        # Loop through and get the categories
                        for cat in self.categories:
                            cat_url = url.replace("<CATEGORY>", cat)

                            # Loop through and Download data for both bytes and count
                            data_list = self.download_data(url=cat_url, start_day=strt_date, category=cat)

                            if cat == "size":
                                # load list
                                for evnt in data_list:
                                    data_bytes.append(evnt)
                            else:
                                for evnt in data_list:
                                    data_count.append(evnt)

        elif 'd' in self.time_ago:
            past_days = int(self.time_ago.replace('d', ""))

            
            for i in range(0,past_days):
                # Prepare date for data collection
                DAY = str(-past_days + i)
                date_obj = datetime.datetime.now()
                new_date = date_obj + datetime.timedelta(days=eval(DAY))

                start_day = f"{new_date.strftime('%Y-%B-%d')}+00:00:00"
                end_day = f"{new_date.strftime('%Y-%B-%d')}+23:59:59"

                # Fix url timespan
                url = new_url.replace('<FIRST_TIME>', start_day)
                url = url.replace('<LAST_TIME>', end_day)
                
                for cat in self.categories:
                    cat_url = url.replace("<CATEGORY>", cat)

                    # Loop through and Download data for both bytes and count
                    data_list = self.download_data(url=cat_url, start_day=start_day, category=cat)

                    if cat == "size":
                        # load list
                        for evnt in data_list:
                            data_bytes.append(evnt)
                    else:
                        for evnt in data_list:
                            data_count.append(evnt)
        
        return data_bytes, data_count 

    def download_data(self, **kwargs):
        # Gather requierd data
        url = kwargs['url']
        start_day = kwargs['start_day']
        category = kwargs['category']

        data = []

        try:
            print(f'url: {url}\n')
            response = requests.get(url, auth=(username, password))
            print(f"{response}\n")
            response_data = response.text
            response_data = response_data.split("\r")
           
            j = 0

            for data_ in response_data: 
                got_data = False
                if j != 0:
                    line_data = data_.split("  ")
                    
                    vol_data = []
                    for item in line_data:
                        if 'count=' in item:
                            if category == "size":
                                count = int((item.split('='))[1])
                                count = count  / (2**30)
                            else:
                                count = (item.split('='))[1]

                            vol_data.append(count)
                            got_data = True
                        elif "value=" in item:
                            vol_data.append((item.split("="))[1])
                        elif "group=" in item:
                            vol_data.append(start_day)

                    if got_data:
                        data.append(vol_data)

                j= j+1

        except Exception as e:
            print(f'Failed to collect data: {e}')
        
        return data

    def build_query(self, **kwargs):
        url = self.url_template

        # Add fieldname
        url = url.replace("<FIELD_NAME>", self.fieldname)

        # Add port
        url = url.replace("<PORT>", self.port)
        
        # Edit size output of return events
        url = url.replace('<SIZE>', str(self.size))

        return url

def query_builder(**kwargs):
    # Required variables
    values = kwargs['values']
    i = 0
    main_text = ''
    for text in values:
        main_text=f"{main_text}{str(i)}) {text}\n"
        i=i+1
    return main_text

def get_date(**kwargs):
    part=kwargs['part']
    # Select the start day
    day = int(input(f"\n\nWhat day would you like to {part} searching data for:\n"))

    # Get the month
    pre_text = f"\n\nWhich month would you like to {part} searching for data:\n"
    main_text=""
    i=1
    for frame in months:
        main_text=f"{main_text}{str(i)}) {frame}\n"
        i=i+1
    suf_text = 'Please only select the number of your option\n'
    
    month = int(input(f'{pre_text}{main_text}{suf_text}\n'))

    # Get the year
    pre_text = f"\n\nWhich year would you like to {part} searching for data:\n"
    main_text=""
    i=1
    for frame in years:
        main_text=f"{main_text}{str(i)}) {frame}\n"
        i=i+1
    suf_text = 'Please only select the number of your option\n'
    
    year = int(input(f'{pre_text}{main_text}{suf_text}\n'))

    if part == 'start':
        
        date_str=f"{years[year-1]}-{month}-{day}"
    else:
        date_str=f"{years[year-1]}-{month}-{day}"

    return date_str

# Place holders
start_date = None 
end_date = None

# build query for the field clause
pre_text = "\n\nFor which meta key would you like to view data for:\n"
main_text1 = query_builder(values=meta_keys)
suf_text = 'Please only select the number of your option\n'

# Build query for date clause
pre_text = "\n\nFor which timeframe would you like to collect log volume:\n"
main_text2= query_builder(values=timeframes)
suf_text = 'Please only select the number of your option\n'

# Build query for time gap we are going to collect data for
pre_text = "\n\nHow far back would you like to collect data:\n"
main_text3= query_builder(values=how_long_ago)
suf_text = 'Please only select the number of your option\n'

# build query for the filter
pre_text = "\n\nFor which meta key would you like to filter data with:\n"
main_text4 = query_builder(values=meta_keys)
suf_text = 'Please only select the number of your option\n'


# Let the program know what arguments are needed
parser.add_argument('--get_data', help=f"Here are you options: \n{main_text1}\nSelect only the number", type=int)
parser.add_argument('--timeframe', help=f"Choose a date, here are your options: \n{main_text2}Select only the number", type=int)
parser.add_argument('--look_back', help=f"Choose a look back period, here are your options: \n{main_text3}Select only the number", type=int)
parser.add_argument('--start_date', help=f"Enter the date in the following format: yy-mm-dd", default='2023-04-05')
parser.add_argument('--end_date', help=f"Enter the date in the following format: yy-mm-dd", default='2023-04-05')
parser.add_argument('--use_filter', help=f"Would you like to filter for a specific value:\n1) Yes\n 2) No\n", type=int, default='2')
parser.add_argument('--filter_key', help=f"Choose a filter key, here are your options: \n{main_text4}Select only the number", type=int)
parser.add_argument('--filter_value', help=f'Enter filter value (if you would like to select multiple filters separate them with a comma): \n', default='')
parser.add_argument('--timestamp_file', help=f'Please enter the timestamp file: \n', default='')

# # Get arguments parser
args = parser.parse_args()

field_clause = args.get_data
date_clause = args.timeframe
time_gap = args.look_back
start_date = args.start_date
end_date = args.end_date
filter_op = args.use_filter
filter_clause = args.filter_key
filter_value = args.filter_value
timestamp_file = args.timestamp_file

keep_looping = True
while keep_looping:
    try:
        print(f'{cwd}{timestamp_file}')
        with open(f'{cwd}{timestamp_file}', 'r') as file:
            last_download_date = file.readlines()[0].replace('\n', '')
            print(f"{last_download_date}")

        # Organise dates
        download_hour = "02:00:00"
        print(f"{last_download_date} {download_hour}")
        last_date = datetime.datetime.strptime(f"{last_download_date} {download_hour}", "%Y-%m-%d %H:%M:%S")
        estimated_next_download = (last_date + datetime.timedelta(days=1))
        next_download_date = datetime.datetime.strptime(f"{last_download_date}", "%Y-%m-%d")

        current_date = datetime.datetime.now()
        if current_date >= estimated_next_download:
            download = True
        else:
            download = False

        if download:
            # savedata immediately after starting to download
            with open(f"{cwd}{timestamp_file}", 'w') as file:
                file.write(f"{str(datetime.datetime.now())[:10]}")
            
            start_date_ = f"{str(next_download_date)[:10]}"
            end_date_ = f"{str(datetime.datetime.now())[:10]}"
            
            print(f"Start date: {start_date}\nEnd date: {end_date}")
 
            if filter_op == 1:

                filter_ = f"{meta_keys[filter_clause-1]}='{filter_value}'+AND+"
                filter_ = filter_value.split(',')
            
                if isinstance(filter_, list):
                    pass
                else:
                    filter_ = list(filter_)
           
            else:
                filter_ = False

                filter_clause=1

            collector = VolumeCollector(filter_=filter_, timeframe=timeframes[date_clause-1], 
                                    fieldname=meta_keys[field_clause-1], url_template=url_template,
                                    port=PORT, hosts=CONCENTRATORS, categories=categories,
                                    time_ago=how_long_ago[time_gap-1], start_date=start_date_,end_date=end_date_,
                                    filter_key=meta_keys[filter_clause-1], size=SIZE)


    except Exception as e:
        print(f"Warning, invalid input: {e}")


    #Replace this with a sleep function. it will sleep for an hour 
    print(f"Sleeping for {str(str(sleep_time/60))} minutes...")
    time.sleep(sleep_time)
    
