import shutil
import time
import os
import sys
from os import path
import pandas as pd
import numpy as np
import matplotlib.ticker as ticker
import matplotlib.pyplot as plt
from datetime import date, timedelta, datetime
from pyspark.sql import types
from pyspark.sql import functions
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession


def init_folder(filename):
    rootdir = 'data//output//files/'
    folder = rootdir + filename
    for current_path, dirs, files in os.walk(rootdir):
        for d in dirs:
            if d == filename:
                shutil.rmtree(folder)
    return folder


def save_to_folder(df, folder, filename):
    plt.savefig('data//output//plots/'+filename+".png", dpi=1200)
    df.coalesce(1).write.format("com.databricks.spark.csv").option(
        "header", "true").save("data//output//files/"+filename)
    plt.close()


def convert_to_dict(output, indexName, valueName):
    df = output.toPandas()
    result = df.set_index(indexName).to_dict()
    return result.get(valueName)


def barchart(inputdict, title, xlabel, ylabel, color):
    plt.title(title)
    labels = inputdict.keys()
    values = inputdict.values()
    plt.bar(labels, values, color='red')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.xticks(fontsize=6)
    plt.xticks(rotation=40)
    # ax.bar(labels, values, width=0.5, color=color)
    # fig.align_labels()
    # plt.show()
    plt.savefig('analysis//static//images/' + queryNumber + '.png')


def piechart(diction, title):
    labels = diction.keys()
    tweet_count = diction.values()
    fig1, ax1 = plt.subplots()
    explode = []
    for label in labels:
        explode.append(0)
    explode[0] = 0.1
    patches, texts = plt.pie(tweet_count, startangle=90, explode=explode)
    # plt.legend(patches, labels, loc='upper right')
    ax1.pie(tweet_count, explode=explode, labels=labels, autopct='%1.1f%%',
            startangle=90)
    ax1.set_title(title)
    plt.savefig('analysis//static//images/' +
                queryNumber + '.png')


def query1():
    print("Executing Query -1")
    # Query 1. Get count of each ecom site
    filename = "query1"
    folder = init_folder(filename)
    count_ecomsite = sc.sql(
        "select ecomsite, count(ecomsite) as count from ecomsitetable group by ecomsite order by count desc LIMIT 10")
    count_ecomsite.show()
    result = convert_to_dict(count_ecomsite, 'ecomsite', 'count')
    barchart(result, 'Number of tweets about each Ecommerce site',
             'Ecom site name', 'No.of tweets', 'r')
    save_to_folder(count_ecomsite, folder, filename)


def query2():
    print("Executing Query -2")
    filename = "query2"
    folder = init_folder(filename)
    countryTweetsCount = sc.sql(
        "SELECT country, count(country) as count from ecomsitetable where country is not null GROUP BY country ORDER BY count DESC LIMIT 10")
    countryTweetsCount.show()
    result = convert_to_dict(countryTweetsCount, 'country', 'count')
    barchart(result, 'Count of tweets from each country',
             'Country Name', 'No.of tweets', 'g')
    # piechart(result, 'Count of tweets from each country')
    save_to_folder(countryTweetsCount, folder, filename)


def query3():
    print("Executing Query -3")
    filename = 'query3'
    folder = init_folder(filename)
    time_data = sc.sql(
        "SELECT SUBSTRING(created_at,12,5) as time_in_hour, COUNT(*) AS count FROM ecomsitetable GROUP BY time_in_hour ORDER BY time_in_hour")
    time_data.show()
    x = pd.to_numeric(time_data.toPandas()["time_in_hour"].str[:2].tolist(
    )) + pd.to_numeric(time_data.toPandas()["time_in_hour"].str[3:5].tolist())/60
    y = time_data.toPandas()["count"].values.tolist()
    tick_spacing = 6
    fig, ax = plt.subplots(1, 1)
    ax.plot(x, y)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))
    plt.title("Tweets Distribution By Minute")
    plt.xlabel("Hours (UTC)")
    plt.ylabel("Number of Tweets")
    plt.savefig('analysis//static//images/' +
                queryNumber + '.png')
    save_to_folder(time_data, folder, filename)


def query4():
    print("Executing Query -4")
    filename = 'query4'
    folder = init_folder(filename)
    total_language_count = sc.sql("SELECT *," +
                                  "CASE when lang LIKE '%en%' then 'EN'" +
                                  "when lang LIKE '%ja%' then 'JP'" +
                                  "when lang LIKE '%es%' then 'ES'" +
                                  "when lang LIKE '%fr%' then 'FR'" +
                                  "when lang LIKE '%it%' then 'IT'" +
                                  "when lang LIKE '%ru%' then 'RU'" +
                                  "when lang LIKE '%ar%' then 'AR'" +
                                  "when lang LIKE '%cs%' then 'CZ'" +
                                  "when lang LIKE '%da%' then 'DN'" +
                                  "when lang LIKE '%de%' then 'DE'" +
                                  "when lang LIKE '%el%' then 'GK'" +
                                  "when lang LIKE '%fa%' then 'PR'" +
                                  "when lang LIKE '%fi%' then 'FI'" +
                                  "when lang LIKE '%fil%' then 'FL'" +
                                  "when lang LIKE '%he%' then 'HE'" +
                                  "when lang LIKE '%hu%' then 'HU'" +
                                  "when lang LIKE '%id%' then 'IN'" +
                                  "when lang LIKE '%ko%' then 'KR'" +
                                  "when lang LIKE '%msa%' then 'MSA'" +
                                  "when lang LIKE '%nl%' then 'NL'" +
                                  "when lang LIKE '%no%' then 'NO'" +
                                  "when lang LIKE '%pl%' then 'PO'" +
                                  "when lang LIKE '%pt%' then 'PR'" +
                                  "when lang LIKE '%ro%' then 'RO'" +
                                  "when lang LIKE '%sv%' then 'SE'" +
                                  "when lang LIKE '%th%' then 'Thai'" +
                                  "when lang LIKE '%tr%' then 'TR'" +
                                  "when lang LIKE '%uk%' then 'UK'" +
                                  "when lang LIKE '%ur%' then 'Urdu'" +
                                  "when lang LIKE '%vi%' then 'VI'" +
                                  "when lang LIKE '%zh-cn%' then 'ZH CN'" +
                                  "when lang LIKE '%zh-tw%' then 'ZH TW'" +
                                  "END AS language from ecomsitetable")
    total_language_count.createOrReplaceTempView("languagetable")
    language_count = sc.sql(
        "SELECT language, count(language) as count from languagetable where language is not null group by language order by count DESC LIMIT 10")
    language_count.show()
    result = convert_to_dict(language_count, 'language', 'count')
    barchart(result, 'Tweets from each Language about Ecommerce sites',
             'Language codes', 'No.of tweets', 'r')
    # piechart(result, 'Tweets from each Language about Ecommerce sites')
    save_to_folder(language_count, folder, filename)


def query5():
    print("Executing Query -5")
    filename = 'query5'
    folder = init_folder(filename)
    category = sc.sql(
        "select category, count(category) as count from categorytable where group By category LIMIT 10")
    category.show()
    result = convert_to_dict(category, 'category', 'count')
    piechart(result, 'Distributions of tweets among different categories')
    save_to_folder(category, folder, filename)


def query6():
    print("Executing Query -6")
    filename = 'query6'
    folder = init_folder(filename)
    hashtagsDF = sc.sql(
        "SELECT explode(hashtags.text) AS hashtagText FROM totalecomsitetable where lang='en'")
    hashtagsDF.registerTempTable("hashtagstable")
    hashtags_count = sc.sql(
        "select distinct hashtagText, count(*) as count from hashtagstable where hashtagText is not null group by hashtagText order by count desc LIMIT 12")
    hashtags_count.show()
    x = hashtags_count.toPandas()['hashtagText'].values.tolist()[1:11]
    y = hashtags_count.toPandas()['count'].values.tolist()[1:11]
    plt.rcParams.update({'axes.titlesize': 'small'})
    plt.barh(x, y, color='green')
    plt.title("Top 10 Hashtags")
    plt.ylabel("Count")
    plt.yticks(fontsize=6)
    plt.yticks(rotation=50)
    plt.xlabel("Hashtags")
    plt.savefig('analysis//static//images/' + queryNumber + '.png')
    save_to_folder(hashtags_count, folder, filename)


def query7():
    print("Executing Query -7")
    filename = 'query7'
    folder = init_folder(filename)
    retweetCount = sc.sql("select ecomsite, count(retweets) as count from ecomsitetable where retweets is not null " +
                          "group BY ecomsite order by count DESC LIMIT 10")
    retweetCount.show()
    result = convert_to_dict(retweetCount, 'ecomsite', 'count')
    barchart(result, 'Retweet count for each Ecom site',
             'Name of E-com site', 'No.of Retweets', 'g')
    save_to_folder(retweetCount, folder, filename)


def query8():
    print("Executing Query -8")
    filename = 'query8'
    folder = init_folder(filename)
    offers_data = sc.sql("select ecomsite, " +
                         "CASE WHEN (text like '%offer%' OR text like '%deal%' OR text like '%exclusive%' " +
                         "OR text like '%best deal%' OR text like '%best offer%') THEN 'OFFERS' END as offers from ecomsitetable")
    offers_data.createOrReplaceTempView("offerstable")
    offers_count = sc.sql(
        "select ecomsite, count(offers) as count from offerstable where offers is not null GROUP BY ecomsite ORDER BY count DESC LIMIT 10")
    offers_count.show()
    result = convert_to_dict(offers_count, 'ecomsite', 'count')
    barchart(result, 'Tweets about offers by each ecomm site',
             'Name of E-com site', 'No.of Tweets about offers', 'b')
    save_to_folder(offers_count, folder, filename)


def query9():
    print("Executing Query -9")
    filename = 'query9'
    folder = init_folder(filename)

    follwers_count = sc.sql(
        "SELECT user.screen_name as screen_name, user.followers_count as count from datatable order by count desc LIMIT 10")
    follwers_count.show()
    x = follwers_count.toPandas()['screen_name'].values.tolist()
    y = follwers_count.toPandas()['count'].values.tolist()
    plt.rcParams.update({'axes.titlesize': 'small'})
    plt.barh(x, y, color='green')
    plt.title("Top 10 People Who Have Most Followers")
    # plt.ylabel("Name")
    plt.yticks(fontsize=6)
    plt.yticks(rotation=40)
    plt.xlabel("Number of followers in crores")
    plt.savefig('analysis//static//images/' +
                queryNumber + '.png')
    save_to_folder(follwers_count, folder, filename)


def query10():
    print("Executing Query -10")
    filename = 'query10'
    folder = init_folder(filename)
    user_created_data = sc.sql(
        "select SUBSTRING(user.created_at, 27,4) as year, count(user.id) as count from datatable where user.created_at is not null group by year order by year desc")
    user_created_data.show()
    x = user_created_data.toPandas()['year'].tolist()
    y = user_created_data.toPandas()['count'].tolist()
    plt.title('Number of Users by year')
    plt.xlabel('Year')
    plt.ylabel('No.of Users')
    plt.xticks(rotation=40)
    plt.xticks(fontsize=6)
    plt.plot(x, y)
    plt.savefig('analysis//static//images/' +
                queryNumber + '.png')
    save_to_folder(user_created_data, folder, filename)


if __name__ == '__main__':
    queryNumber = sys.argv[1]

    sc = SparkSession.builder.appName("Principles of BigData Management").config("spark.sql.shuffle.partitions", "50").config(
        "spark.driver.maxResultSize", "5g").config("spark.sql.execution.arrow.enabled", "true").getOrCreate()

    total_data = sc.read.json('data//input//tweetsdata_v2.txt')
    total_data.registerTempTable("datatable")

    # Ecom site tweets extraction and creating temp table
    total_ecomsite_data = sc.sql("SELECT id as tweet_id, user.id as user_id, user.name as username, lang, user.verified as verified," +
                                 "user.location as location, text, created_at, retweet_count as retweets,entities.hashtags as hashtags," +
                                 "place.country_code as country," +
                                 "CASE WHEN text like '%amazon%' THEN 'AMAZON'" +
                                 "WHEN text like '%flipkart%' THEN 'FLIPKART'" +
                                 "WHEN text like '%walmart%' THEN 'WALMART'" +
                                 "WHEN text like '%snapdeal%' THEN 'SNAPDEAL'" +
                                 "WHEN text like '%ebay%' THEN 'EBAY'" +
                                 "WHEN text like '%etsy%' THEN 'ETSY'" +
                                 "WHEN text like '%home depot%' THEN 'HOME DEPOT'" +
                                 "WHEN text like '%target%' THEN 'TARGET'" +
                                 "WHEN text like '%best buy%' THEN 'BEST BUY'" +
                                 "WHEN text like '%wayfair%' THEN 'WAY FAIR'" +
                                 "WHEN text like '%macys%' THEN 'MACYS'" +
                                 "WHEN text like '%lowes%' THEN 'Lowes'" +
                                 "END AS ecomsite from datatable where text is not null")
    total_ecomsite_data.registerTempTable("totalecomsitetable")
    ecomsite_data = sc.sql(
        "select * from totalecomsitetable where ecomsite is not null")
    ecomsite_data.registerTempTable("ecomsitetable")

    # Extract category based tweets from the  Ecom site table
    total_categories_data = sc.sql("SELECT *," +
                                   "CASE WHEN (text like '%electronics%' or text like '%Electronics%') THEN 'ELECTRONICS'" +
                                   "WHEN (text like '%mobile%' or text like '%phone%') THEN 'MOBILES'" +
                                   "WHEN (text like '%fashion%' or text like '%jeans%' or text like '%shirt%') THEN 'FASHION'" +
                                   "WHEN text like '%book%' THEN 'BOOKS'" +
                                   "WHEN (text like '%beauty%' or text like '%makeup%' or text like '%cosmetics%')THEN 'BEAUTY'" +
                                   "WHEN (text like '%home%' or text like '%house%') THEN 'HOUSEHOLD'" +
                                   "END as category from ecomsitetable")
    total_categories_data.createOrReplaceTempView("totalcategorytable")
    categories_data = sc.sql(
        "select * from totalcategorytable where category is not null")
    categories_data.createOrReplaceTempView("categorytable")

    # Configure function to exectute particluar query

    queryIndex = {
        "1": query1,
        "2": query2,
        "3": query3,
        "4": query4,
        "5": query5,
        "6": query6,
        "7": query7,
        "8": query8,
        "9": query9,
        "10": query10,
    }
    queryIndex.get(queryNumber)()
