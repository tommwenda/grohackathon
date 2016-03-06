import sys
import time
import getopt
import psycopg2
import requests
import locale

#for handling value column. eg 123,000
locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )

filters =['sector_desc','agg_level_desc','year__GE','year__LE']
def begin_nass_harvest(database_host, database_name, database_user, database_password,
                       port, start_date, end_date):
    '''print "\nThe Gro Hackathon's NASS harvester."
    print "Run 'python harvest.py -h' for help\n\n"
    print "Supplied Query: "
    print "Harvest Start Date: {}".format(start_date)
    print "Harvest End Date: {}\n".format(end_date)'''
    '''get row count first'''
    params = get_params(start_date, end_date)
    totalRows = get_rowcount(params)
    totalRows = int(totalRows)
    limit =50000
    while (totalRows > limit):
        screen_out="Records found ("+str(totalRows)+") are more than 50000 please enter a filter. \n eg commodity_desc=CORN \n "
        screen_out+="See http://quickstats.nass.usda.gov/api#param_define for more\n"
        screen_out+="Current query(You can replace the values too): \n"
        for para in params:
            filter_ = "&"+para["key"]+"="+para["value"]
            screen_out+=filter_

        screen_out+="\nEnter filter: "
        filt = raw_input(screen_out)
        filts = filt.split("=")
        if filts[0] not in filters:
            filters.append(filts[0])
            newParam = {"key":filts[0],'value':filts[1]}
            params.append(newParam)
        else:
            for par in params:
                if par['key'] == filts[0]:
                    target_index = next(index for (index, d) in enumerate(params) if d["key"] == filts[0])
                    params[target_index]["value"] = filts[1]


        totalRows = int(get_rowcount(params))

    data = get_data(params)
    port = str(port)
    connString ="dbname='"+database_name+"' user='"+database_user+"' host='"+database_host+"' password='"+database_password+"' port='"+port+"'"
    try:
        conn = psycopg2.connect(connString)
        create_table(conn)
        if totalRows > 0:
            total = insert_data(conn, data)
            do_stats(conn)
            print str(total)+" records stored";
    except psycopg2.OperationalError as e:
        print('Unable to connect!\n{0}').format(e)
        conn.close()
        sys.exit(1)

def get_params(start_date,end_date):
    begin_year = start_date.split('-')[0]
    end_year = end_date.split('-')[0]
    params = [
    {"key":"sector_desc","value":"CROPS"},
    {"key":"agg_level_desc","value":"COUNTY"},
    ]

    #if begin year and end year are supplied search within the boundaries
    if len(begin_year) > 1 and len(end_year) > 1:
        #convert integer years to string
        begin_year = str(begin_year)
        end_year = str(end_year)
        params.append({"key":"year__GE","value":begin_year})
        params.append({"key":"year__LE","value":end_year})

    elif len(begin_year) > 1 and len(end_year)<4:
        #convert integer years to string
        begin_year = str(begin_year)
        params.append({"key":"year","value":begin_year})

    elif len(begin_year) < 4 and len(end_year) > 4:
        #convert integer years to string
        end_year = str(end_year)
        params.append({"key":"year__GE","value":end_year})
    else:
       #do nothing
       print ""

    return params

def make_request(endpoint,params):
    apiKey ="957D1F18-3F24-3CB4-8B8F-7C9779E51469"
    service_url ="http://quickstats.nass.usda.gov/api/"+endpoint+"?key="+apiKey
    requestString = ""
    #build the quey string
    for param in params:
        requestString+="&"+param['key']+"="+param['value']

    service_url+=requestString

    #Show progress
    processing ="Processing"
    if endpoint=="get_counts":
        processing="Counting records"

    if endpoint =="api_GET":
        processing ="Fetching records"
    print processing+"...\\",
    syms = ['\\', '|', '/', '-']
    bs = '\b'
    fetching = True
    while fetching:
        for sym in syms:
            sys.stdout.write("\b%s" % sym)
            sys.stdout.flush()
            time.sleep(.1)
        resp = requests.get(service_url)
        fetching = False
        break

    if resp.status_code != 200:
        print "Bad Request \n"
        print service_url


    #print service_url

    return resp.json()

def get_param_values(params):
    endpoint = "get_param_values"
    response = make_request(endpoint, params)
    return response

def get_rowcount(params):
    endpoint = "get_counts"
    response = make_request(endpoint, params)
    print " Found "+str(response["count"])+" records"
    return response["count"]

def get_data(params):
    endpoint = "api_GET"
    response = make_request(endpoint, params)
    if "error" in response:
        print response['error']
        sys.exit(1)
    print " Records fetching completed \n"
    return response["data"]

def insert_data(conn, data):
    cursor = conn.cursor()
    #clean table
    cursor.execute("DELETE FROM fact_data")
    conn.commit()
    sql ="INSERT INTO fact_data (domain_desc, commodity_desc, statisticcat_desc, agg_level_desc,country_name,state_name,county_name,unit_desc,value,year) VALUES"
    values = []
    print "Saving records..."
    for datum in data:
        value = datum['Value'].strip()
        if value=="(D)":
            value ="(D)"
        else:
            value = str(locale.atof(value))

        vals = "('"+datum['domain_desc']+"','"+datum['commodity_desc']+"','"+datum['statisticcat_desc']+"','"+datum['agg_level_desc']+"','"+datum['country_name']+"','"+datum['state_name']+"','"+datum['county_name']+"','"+datum['unit_desc']+"','"+value+"','"+datum['year']+"')"
        values.append(vals)

    sql+=" "+",".join(values)
    cursor.execute(sql)
    rowcount = cursor.rowcount;
    conn.commit()
    print "Saving records completed! \n"
    return rowcount

def do_stats(conn):
    print "Doing some stats..."
    cursor = conn.cursor();
    #clean table
    cursor.execute("DELETE FROM stats")
    conn.commit()
    #crop totals
    cursor.execute("select commodity_desc, max(CAST(value as DECIMAL)) as total from fact_data  WHERE value<> '(D)' GROUP BY commodity_desc ")
    data = cursor.fetchall()
    sql ="INSERT INTO stats values "
    values = []
    for datum in data:
        val = str(datum[1])
        value ="('Commodity Totals','"+datum[0]+"','"+val+"')"
        values.append(value)

    sql+=",".join(values)
    print sql
    cursor.execute(sql)
    conn.commit()
    print "Operation completed"

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS fact_data
       (
       domain_desc          TEXT    NOT NULL,
       commodity_desc       TEXT     NOT NULL,
       statisticcat_desc    TEXT,
       agg_level_desc       TEXT,
       country_name         TEXT,
       state_name           TEXT,
       county_name          TEXT,
       unit_desc            TEXT,
       value                character varying,
       year            CHAR(4) NOT NULL);''')
    conn.commit()
    cursor.execute('''CREATE TABLE IF NOT EXISTS stats
      (
      title          TEXT    NOT NULL,
      stat       TEXT     NOT NULL,
      value            TEXT NOT NULL);''')

    conn.commit()



# #################################################
# PUT YOUR CODE ABOVE THIS LINE
# #################################################
def main(argv):
    try:
        opts, args = getopt.getopt(argv, "h", ["database_host=", "database_name=", "start_date=",
                                               "database_user=", "database_pass=", "end_date="])
    except getopt.GetoptError:
        print 'Flag error. Probably a mis-typed flag. Make sure they start with "--". Run python ' \
              'harvest.py -h'
        sys.exit(2)

    #define defaults
    database_host = 'localhost'
    database_name = 'fact_data'
    port = 5433
    database_user = 'postgres'
    database_password = 'eurobond'
    start_date = ''
    end_date = ''

    for opt, arg in opts:
        if opt == '-h':
            print "\nThis is my harvest script for the Gro Hackathon NASS harvest"
            print '\nExample:\npython harvest.py --database_host localhost --database_name gro2\n'
            print '\nFlags (all optional, see defaults below):\n ' \
              '--database_host [default is "{}"]\n ' \
              '--database_name [default is "{}"]\n ' \
              '--database_user [default is "{}"]\n ' \
              '--database_pass [default is "{}"]\n ' \
              '--start_date [default is "{}"]\n ' \
              '--end_date [default is "{}"]\n'.format(database_host, database_name, database_user,
                                                      database_password, start_date, end_date)
            sys.exit()
        elif opt in ("--database_host"):
            database_host = arg
        elif opt in ("--database_name"):
            database_name = arg
        elif opt in ("--database_user"):
            database_user = arg
        elif opt in ("--database_pass"):
            database_password = arg
        elif opt in ("--start_date"):
            start_date = arg
        elif opt in ("--end_date"):
            end_date = arg

    begin_nass_harvest(database_host, database_name, database_user, database_password,
                       port, start_date, end_date)

if __name__ == "__main__":
   main(sys.argv[1:])
