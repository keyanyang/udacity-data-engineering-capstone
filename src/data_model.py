
import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *


def create_i94_fact(df_i94):
    """
    Create I94 fact table using processed dataframe
    """
    res = df_i94.drop('i94visa')
    
    # convert numeric to integer
    for col_int in ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'arrdate', 'i94mode', 'depdate',
                   'i94bir', 'count', 'biryear', 'dtaddto', 'admnum']:
        res = res.withColumn(col_int, res[col_int].cast(IntegerType()))
    
    # convert date in SAS format to datetime object
    get_datetime = udf(lambda x: (datetime.datetime(1960, 1, 1).date() + datetime.timedelta(x)).isoformat() if x else None)
    res = res.withColumn("arrdate", get_datetime(res['arrdate']))
    res = res.withColumn("depdate", get_datetime(res['depdate']))
    
    # convert character date to datetime object
    res = res.withColumn("dtadfile", to_date(res['dtadfile'], 'yyyyMMdd'))
    
    return res


def create_visa_dim(df_i94):
    """
    Create Visa dimension table using processed dataframe
    """
    res = df_i94.select(['visatype', 'i94visa']).distinct()
    res = res.withColumn("i94visa", res["i94visa"].cast(IntegerType()))
    return res.select(['visatype', 'i94visa'])


def create_temperature_dim(df_temp, i94port_name_code_dict):
    """
    Create Temperature dimension table using processed dataframe
    """
    @udf()
    def get_i94port(city):
        """
        Get city code for provided city name
        """
        for name, code in i94port_name_code_dict.items():
            if city and city.upper() in name:
                return code

    res = df_temp.filter(upper(df_temp['city']).isin(list(i94port_name_code_dict.keys())))
    res = df_temp.select(['city', 'country']).withColumnRenamed("City", "city") \
            .withColumnRenamed("Country", "country")
    res = df_temp.groupBy(['city', 'country']).agg(mean('AverageTemperature').alias("average_temperature"))
    # add i94port codea
    res = res.withColumn("i94port", get_i94port(res.city))

    return res


def create_airport_dim(df_airport, i94port_name_code_dict):
    """
    Create Airport dimension table using processed dataframe
    """
    @udf()
    def get_i94port(city):
        """
        Get city code for provided city name
        """
        for name, code in i94port_name_code_dict.items():
            if city and city.upper() in name:
                return code

    res = df_airport.drop('continent')
    
    # add i94port code
    split_iso_region = split(res['iso_region'], '-')
    res = res.withColumn('state', split_iso_region.getItem(1))
    res = res.withColumn("city_state", concat(res['municipality'], lit(', '), res['state']))
    res = res.withColumn("i94port", get_i94port(res.city_state))
    res = res.drop('city_state')
    return res


def create_demographics_dim(df_demo, i94port_name_code_dict):
    """
    Create Demographics dimension table using processed dataframe
    """
    @udf()
    def get_i94port(city):
        """
        Get city code for provided city name
        """
        for name, code in i94port_name_code_dict.items():
            if city and city.upper() in name:
                return code

    res = df_demo.withColumnRenamed('City','city') \
            .withColumnRenamed('State','state') \
            .withColumnRenamed('Median Age','median_age') \
            .withColumnRenamed('Male Population', 'male_population') \
            .withColumnRenamed('Female Population', 'female_population') \
            .withColumnRenamed('Total Population', 'total_population') \
            .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
            .withColumnRenamed('Foreign-born', 'foreign_born') \
            .withColumnRenamed('Average Household Size', 'average_household_size') \
            .withColumnRenamed('State Code', 'state_code')
    
    # add i94port code
    res = res.withColumn("city_state", concat(res['city'], lit(', '), res['state_code']))
    res = res.withColumn("i94port", get_i94port(res.city_state))
    res = res.drop('city_state')

    # reorder cols
    cols = res.columns
    res = res.withColumn('id', monotonically_increasing_id())
    return res.select(['id'] + cols)
