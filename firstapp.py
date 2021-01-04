import psycopg2
import pandas as pd
import plotly.express as px
import datetime as dt
import streamlit as sl
import geopandas as gpd
import numpy as np
import base64 as base64

##
##SET UP CONNECTION TO DATABASE & LOAD SEATTLE NEIGHBORHOODS SPATIAL FRAME
##

def get_table_download_link(df):
    """Generates a link allowing the data in a given panda dataframe to be downloaded
    in:  dataframe
    out: href string
    """
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
    href = f'<a href="data:file/csv;base64,{b64}">Download csv file</a>'
    return(href)



config_plotly = {'displayModeBar': False}
slim = dict(l = 10, r = 10, b = 15, t = 35, pad = 0)

seattle_hoods_gdf = gpd.read_file("seattle_neighborhoods/City_Clerk_Neighborhoods.shp")


connection = psycopg2.connect(user = "jcaropostgres",
                             password = "$in3$in3",
                             host = "jcarotest.cmu75txnffne.us-west-1.rds.amazonaws.com" ,
                             port = "5432",
                             database = "testing")
##
## QUERY DATABASE VIA USER INPUT
##
reference_date = dt.date.today() - dt.timedelta(days = 28)
dates_slicer = [(reference_date - dt.timedelta(i)).strftime("%Y-%m-%d") for i in range(31)]

start_date = sl.sidebar.select_slider("Select Number of days :",options = dates_slicer)

query_str = "select type, objectid,datetime, cast(datetime as date) as date from public.seattlefirecalls_geocoded\
                    where CAST(datetime as date) >= '" + start_date + "'"

df = pd.read_sql_query(query_str, connection)



## OVERALL INCIDENT COUNTS


### OVERALL INCIDENT TYPE COUNT
type_counts = df.groupby(["type"], as_index = False).\
                    agg(total_counts = pd.NamedAgg("type", "count")).\
                    sort_values("total_counts", ascending = False)

type_counts = type_counts.reset_index().drop(columns = "index").nlargest(15, "total_counts")

type_class = ["All"]
type_class.extend(type_counts["type"].drop_duplicates().to_list())

incident_select = sl.sidebar.selectbox("Select Incident Type:", type_class)


sl.markdown("# Seattle Emergency Dispatch Analytics")
num_records = format(len(df),",")
summary_count_str = "There have been **"+ num_records + "** incident records identified within Seattle neighborhoods  \nsince " + start_date

sl.markdown(summary_count_str)

sl.markdown("To include earlier dates, expand search criteria, or find out more about this app and it's owner,\
                    you may expand the pane to the left by clicking the arrow.")

sl.sidebar.markdown("The source data contained herein is public data facilitated by the city of seattle\
                    via [data.seattle.gov](https://data.seattle.gov). Thorough documentation illustrating all\
                    methodology and processes is currently being written, and will be cited here. You\
                    may contact the owner [here](mailto:jmcaro2@icloud.com)")
##
## Incident Count Map
##

title_map = "Incident Count by Neighborhoods Since "+ start_date

count_incidents = df
if incident_select != "All":

    count_incidents = df[df["type"] == incident_select]

    

count_incidents = count_incidents.groupby("objectid", as_index = False).\
                    agg(incident_counts = pd.NamedAgg("objectid", "count"))

count_incidents = count_incidents.\
                    assign(OBJECTID = lambda x: x["objectid"].astype("int64")).\
                    merge(seattle_hoods_gdf, how = "left", on = "OBJECTID")
count_incidents = gpd.GeoDataFrame(count_incidents,
                                         geometry = count_incidents.geometry,
                                  crs = "EPSG:4326")

count_incidents_fig = px.choropleth(count_incidents, geojson = count_incidents.geometry,
                          locations = count_incidents.index,
                         color = count_incidents.incident_counts,
                         hover_name = count_incidents.S_HOOD,
                         template = "ggplot2",
                         title = title_map)

#coloraxis_showscale=False

count_incidents_fig.update_geos(fitbounds="locations", visible=False)
count_incidents_fig.update_layout(margin = slim , autosize = False)
count_incidents_fig.update_yaxes(automargin = True)
count_incidents_fig.update_coloraxes(colorbar = dict(yanchor = "bottom", xanchor = "left", len = 0.25, lenmode = "fraction"))

sl.plotly_chart(count_incidents_fig,
                use_container_width=True,
               config = config_plotly)

##
## Incident Treemaps
##


title_incident_tree = "Treemap of Incident Counts by Type, Seattle Region & Neighborhood <br> Since " + start_date
incident_tree = df

incident_tree["city"] = "Seattle"

if incident_select != "All":
    incident_tree = incident_tree[incident_tree["type"] == incident_select]

seattle_hoods_cond = seattle_hoods_gdf.loc[:,["S_HOOD", "L_HOOD", "OBJECTID"]]

seattle_hoods_cond.columns = map(str.lower, seattle_hoods_cond.columns)

seattle_hoods_cond = seattle_hoods_cond[seattle_hoods_cond["s_hood"] != "OOO"]

df_with_hoods = incident_tree.merge(seattle_hoods_cond, how = "inner", on = "objectid")

df_with_hoods = df_with_hoods.groupby(["city", "s_hood", "l_hood", "type"], as_index = False).\
                    agg(incidents = pd.NamedAgg("type", "count"))

path_vars = [ "Region", "Incident Type"]

principle_var = sl.selectbox("Select Principle Variable :", path_vars)

if principle_var == "Incident Type":
    tree_path = ["city", "type" ,"l_hood", "s_hood"]
else:
    tree_path = ["city","l_hood", "s_hood", "type" ]

incident_treemap = px.treemap(df_with_hoods,
                              path = tree_path,
                              values = "incidents",
                              color = "incidents",
                              color_continuous_scale = "YlGnBu",
                              title = title_incident_tree)
incident_treemap.update_layout( margin = dict(l = 10,
                                              r = 10,
                                              b = 15,
                                              t = 70,
                                              pad = 0),
                               title_x=0.5)

sl.plotly_chart(incident_treemap,
                config = config_plotly,
                use_container_width=True)
##
## Type Count Barchart
##

title_type_counts = "Count of Incidents by Type Since " + start_date

if incident_select != "All":
    
    type_counts = type_counts.\
                        assign(selected = lambda x: np.where(x["type"] == incident_select, 1, 0.25))
else:
    type_counts = type_counts.\
                        assign(selected = 1)

bar_chart_type_counts = px.bar(type_counts,
                               opacity = type_counts["selected"],
                               x = "type",
                               y = "total_counts",
                              title = title_type_counts)

bar_chart_type_counts.update_layout(yaxis_categoryorder = 'total ascending',
                                    margin = slim,
                                    title = title_type_counts,
                                    title_x = 0.5)

sl.plotly_chart(bar_chart_type_counts,
                use_container_width=True,
                config = config_plotly)


##
## Hour-wise boxplots
##

boxplot_title = "Boxplot of Incidents by Hour Since " + start_date 

hour_wise = df

if incident_select != "All":
    hour_wise = hour_wise[hour_wise["type"] == incident_select]

hour_wise["date"] = hour_wise["datetime"].astype("datetime64[D]")

hour_wise["hour"] = hour_wise["datetime"].dt.hour
hour_counts = hour_wise.groupby(["date", "hour"], as_index = False).\
                    agg(hour_counts = pd.NamedAgg("hour", "count"))

hour_wise_box_plot = px.box(hour_counts,
                            x = "hour",
                            y = "hour_counts")

hour_wise_box_plot.update_layout(margin = slim,
                            title = boxplot_title,
                            title_x = 0.5)

sl.plotly_chart(hour_wise_box_plot,
                use_container_width = True,
                config = config_plotly)
##
## Incident Trend Lineplot
##

incident_trend_title = "Trend of Incidents Since " + start_date

incident_trend = df

incident_trend["date"] = incident_trend["date"].astype("datetime64[D]")



if incident_select != "All":
    
    incident_trend = incident_trend[incident_trend["type"] == incident_select]


incident_trend = incident_trend.groupby("date", as_index = False).\
                    agg(incident_counts = pd.NamedAgg("date", "count"))


incident_trend_plt = px.line(incident_trend, x = "date", y = "incident_counts")

incident_trend_plt.update_layout(legend = dict(yanchor = "bottom", xanchor = "right"),
                                 margin = slim,
                                 title = incident_trend_title,
                                 title_x = 0.5)

sl.plotly_chart(incident_trend_plt,
                use_container_width=True,
                config = config_plotly)


##
## Raw data & Incident-Level Map
##

view_raw_data = sl.checkbox('View Incident Map & Raw Data')


if view_raw_data:
    
    desc_raw_data = "Incidents for all types are illustrated on the scatter plot, and supplied in the table below.\
    This data is through **" + start_date + "** and can be changed through the user input pane to the left." 
    
    sl.markdown(desc_raw_data)

    query_str_raw = "select * from public.seattlefirecalls_geocoded psf \
left join (select * from public.communities_shape) cs \
on cs.OBJECTID = psf.objectid \
where CAST(datetime as date) >= '" + start_date + "'"
    
    df_raw = pd.read_sql_query(query_str_raw, connection)

    sl.map(df_raw)
    df_raw = df_raw.loc[:,["address", "type", "datetime", "datetime_pst", "incident_number", "S_HOOD", "L_HOOD", "latitude", "longitude" ]]
    sl.dataframe(df_raw)
    sl.markdown(get_table_download_link(df_raw), unsafe_allow_html=True)


sl.markdown("Ran and maintained by [J. Caro](http://www.jesuscaro.org)")