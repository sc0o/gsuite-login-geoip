import pandas as pd
import argparse
import plotly
import progressbar
import random
import geoip2.database

def main():
    # count records
    with open(infile) as f:
        rec_count = sum(1 for _ in f) - 1
    print('Processing {} log entries... Please wait.'.format(rec_count))

    df = pd.read_csv(
            infile,
            encoding='utf-8',
            header=0,
            usecols=["Event Description", "IP Address", "Date"],
            parse_dates=["Date"]
            )
    reader = geoip2.database.Reader(GeoLiteCity) 
    i = 0
    totalrows = len(df.index)
    bar = progressbar.ProgressBar(max_value=totalrows).start()
    for idx,row in df.iterrows():
        i += 1
        bar.update(i)
        ipAddress = row["IP Address"]
        try:
            #geodata = geoip_city(ipAddress)
            geodata = reader.city(ipAddress)

        except (ValueError, geoip2.errors.AddressNotFoundError) as e:
            print(e)
            print('ip: {}'.format(ipAddress))
            
        except Exception as e:
            print(e)
            print('idx: {}'.format(idx))
        
        else:
            df.loc[df.index[idx], 'latitude'] = geodata.location.latitude
            df.loc[df.index[idx], 'longitude'] = geodata.location.longitude
            df.loc[df.index[idx], 'city'] = geodata.city.name
            df.loc[df.index[idx], 'region_code'] = geodata.subdivisions.most_specific.iso_code
            df.loc[df.index[idx], 'country_name'] = geodata.country.name
            df.loc[df.index[idx], 'postal_code'] = geodata.postal.code

            long_desc = '{} - {} from {}, {}, {}'.format(row["Date"], row["Event Description"], df.loc[df.index[idx], "city"], df.loc[df.index[idx], "region_code"], df.loc[df.index[idx], "country_name"])

            df.loc[df.index[idx], "long_desc"] = long_desc

            for coord in ['latitude','longitude']:
                df.loc[df.index[idx], coord] = scatterlatlong(df.loc[df.index[idx], coord])

    bar.finish()
    buildmap(df)

def scatterlatlong(coord):
    # neccesary to prevent overlapping coords
    coordmax = coord + 0.05
    coordmin = coord - 0.05
    return random.uniform(coordmin,coordmax)

def geoip_city(ipAddress):
    gic = geoip2.database.Reader(GeoLiteCity)
    return gic.city(ipAddress)

def getColor(eventType,returnType):

    levels = {
        'warning': {
            'color': "Red",
            'opacity': 1,
            'size': 8,
            'triggers': ["failed"]
            },
        'caution': {
            'color': "Orange",
            'opacity': 0.8,
            'size': 7,
            'triggers': ["challenge"]
            },
        'common': {
            'color': "Blue",
            'opacity': 0.6,
            'size': 6,
            'triggers': ["logged in", "logged out"]
            },
        }

    for level in levels:
        for trigger in levels[level]['triggers']:
            if trigger in eventType:
                for availableType in ['color','opacity','size']:
                    if returnType == availableType:
                        return levels[level][availableType]
            else:
                pass # worst error handler ever

    return "Black" # worst error handler ever

def buildmap(df):
    for idx,row in df.iterrows():
        eventType = row["Event Description"]
        if eventType:
            for markerattr in ['color','opacity','size']:
                df.loc[df.index[idx], markerattr] = getColor(eventType,markerattr)
        else: # shouldn't happen
            df.loc[df.index[idx], "color"] = "Pink"
            df.loc[df.index[idx], "opacity"] = 0.5
            df.loc[df.index[idx], "size"] = 6

    data = [ dict(
            type = 'scattergeo',
            lon = df['longitude'],
            lat = df['latitude'],
            text = df['long_desc'],
            mode = 'markers',
            marker = dict(
                size = df['size'],
                opacity = df['opacity'],
                reversescale = True,
                autocolorscale = False,
                symbol = 'circle',
                line = dict(
                    width=1,
                    color='rgba(102, 102, 102)'
                    ),
                color = df['color'],
                )
            )
        ]

    layout = dict(
            title = maptitle,
            colorbar = False,
            geo = dict(
                scope='world',
                projection=dict( type='equirectangular' ),
                showland = True,
                showcountries = True,
                showsubunits = True,
                showcoastlines = True,
                landcolor = "rgb(250, 250, 250)",
                subunitcolor = "rgb(217, 217, 217)",
                countrycolor = "rgb(217, 217, 217)",
                coastlinecolor = "rgb(220, 220, 220)",
                countrywidth = 0.5,
                subunitwidth = 0.5,
                showframe = False
            ),
        )

    fig = dict( data=data, layout=layout )
    plotly.offline.plot( fig, validate=False, filename='map.html' )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("inputfile", help="The CSV export from GSuite Login \
                                            Activity Report")
    parser.add_argument("geocitydb", help="Path to the GeoLite2-City.mmdb \
                                            database")
    parser.add_argument("--maptitle", help="Title to be displayed on the map")
    args = parser.parse_args()
    infile = args.inputfile
    GeoLiteCity = args.geocitydb
    if args.maptitle:
        maptitle = args.maptitle
    else:
        maptitle = "GSuite Login Activity"
    main()
